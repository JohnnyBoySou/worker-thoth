#!/usr/bin/env bash
#
# Teste de carga self-contained do worker-thoth.
#
# Estressa o MESMO código do worker (intake HTTP, fila Redis, downloads
# concorrentes, backpressure, retorno de memória) SEM tocar na produção nem no
# Whisper real. Sobe: Redis (docker), um Whisper FALSO com latência configurável
# (simula a GPU ocupada) e um servidor local que serve um áudio REAL baixado da
# web (exercita Content-Length + LimitReader + streaming de download).
#
# Uso:    ./scripts/loadtest.sh
# Tunáveis por env (com defaults):
#   REQUESTS=500        # total de requisições disparadas
#   SENDERS=50          # concorrência do gerador de carga (curl paralelos)
#   MAX_CONCURRENCY=8   # workers do pool (chamadas simultâneas ao Whisper)
#   QUEUE_SIZE=200      # capacidade da fila antes de 429 (backpressure)
#   WHISPER_MS=150      # latência simulada do Whisper falso (ms)
#
# Requisitos: go, docker, curl, python3.

set -euo pipefail

REQUESTS="${REQUESTS:-500}"
SENDERS="${SENDERS:-50}"
MAX_CONCURRENCY="${MAX_CONCURRENCY:-8}"
QUEUE_SIZE="${QUEUE_SIZE:-200}"
WHISPER_MS="${WHISPER_MS:-150}"

REDIS_PORT="${REDIS_PORT:-6392}"
MOCK_PORT="${MOCK_PORT:-8798}"
AUDIO_PORT="${AUDIO_PORT:-8797}"
WORKER_PORT="${WORKER_PORT:-8772}"
API_KEY="${API_KEY:-load-test-key}"
REDIS_NAME="thoth-load-redis"
AUDIO_URL_WEB="${AUDIO_URL_WEB:-https://download.samplelib.com/mp3/sample-15s.mp3}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
BIN="$TMP/worker"

c_green=$'\033[0;32m'; c_red=$'\033[0;31m'; c_blue=$'\033[0;34m'; c_yel=$'\033[0;33m'; c_off=$'\033[0m'
info() { echo "${c_blue}==>${c_off} $*"; }
ok()   { echo "  ${c_green}OK${c_off} $*"; }
warn() { echo "  ${c_yel}!!${c_off} $*"; }

cleanup() {
  set +e
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null
  [[ -n "${MOCK_PID:-}" ]]   && kill "$MOCK_PID" 2>/dev/null
  [[ -n "${AUDIO_PID:-}" ]]  && kill "$AUDIO_PID" 2>/dev/null
  docker rm -f "$REDIS_NAME" >/dev/null 2>&1
  rm -rf "$TMP"
}
trap cleanup EXIT

wait_http() { local url="$1" deadline=$(( SECONDS + ${2:-15} ))
  until curl -sf -o /dev/null "$url" 2>/dev/null; do
    (( SECONDS >= deadline )) && { echo "timeout esperando $url"; return 1; }
    sleep 0.3
  done
}

# =====================================================================
info "config: REQUESTS=$REQUESTS SENDERS=$SENDERS MAX_CONCURRENCY=$MAX_CONCURRENCY QUEUE_SIZE=$QUEUE_SIZE WHISPER_MS=${WHISPER_MS}ms"

info "1/5  build"
cd "$ROOT"
go build -o "$BIN" ./cmd/server && ok "go build"

info "2/5  baixando áudio real da web (uma vez) e subindo dependências"
curl -sf -L "$AUDIO_URL_WEB" -o "$TMP/audio.mp3" && ok "áudio: $(wc -c < "$TMP/audio.mp3") bytes de $AUDIO_URL_WEB"

docker rm -f "$REDIS_NAME" >/dev/null 2>&1 || true
docker run -d --name "$REDIS_NAME" -p "$REDIS_PORT:6379" redis:7-alpine >/dev/null
ok "redis em :$REDIS_PORT"

# Whisper falso: latência fixa + resposta JSON válida (simula a GPU ocupada).
cat > "$TMP/mock.py" <<PY
import http.server, json, time
DELAY = $WHISPER_MS / 1000.0
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length",0)))
        time.sleep(DELAY)
        b=json.dumps({"text":"carga","language":"pt","elapsed_ms":int(DELAY*1000)}).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*a): pass
import socketserver
class S(socketserver.ThreadingMixIn, http.server.HTTPServer): daemon_threads=True
S(("127.0.0.1",$MOCK_PORT),H).serve_forever()
PY
python3 "$TMP/mock.py" & MOCK_PID=$!

# Servidor de áudio: serve o mp3 real baixado, com Content-Length correto.
cat > "$TMP/audiosrv.py" <<PY
import http.server, os
DATA = open("$TMP/audio.mp3","rb").read()
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header("Content-Type","audio/mpeg")
        self.send_header("Content-Length",str(len(DATA))); self.end_headers(); self.wfile.write(DATA)
    def log_message(self,*a): pass
import socketserver
class S(socketserver.ThreadingMixIn, http.server.HTTPServer): daemon_threads=True
S(("127.0.0.1",$AUDIO_PORT),H).serve_forever()
PY
python3 "$TMP/audiosrv.py" & AUDIO_PID=$!
sleep 0.5
ok "whisper falso :$MOCK_PORT (${WHISPER_MS}ms) | servidor de áudio :$AUDIO_PORT"

info "3/5  iniciando worker (MAX_CONCURRENCY=$MAX_CONCURRENCY QUEUE_SIZE=$QUEUE_SIZE)"
API_KEY="$API_KEY" WHISPER_API_KEY="up" \
  REDIS_URL="redis://localhost:$REDIS_PORT/0" \
  WHISPER_UPSTREAM_URL="http://127.0.0.1:$MOCK_PORT" \
  PORT="$WORKER_PORT" MAX_CONCURRENCY="$MAX_CONCURRENCY" QUEUE_SIZE="$QUEUE_SIZE" \
  JOB_TTL=30m \
  "$BIN" > "$TMP/worker.log" 2>&1 & WORKER_PID=$!
wait_http "http://localhost:$WORKER_PORT/health" 15 && ok "worker em :$WORKER_PORT"

# =====================================================================
info "4/5  disparando $REQUESTS requisições (concorrência $SENDERS)"
AUDIO_URL="http://127.0.0.1:$AUDIO_PORT/clip.mp3"
CODES="$TMP/codes.txt"; LAT="$TMP/lat.txt"; : > "$CODES"; : > "$LAT"

send_one() { # imprime "<http_code> <tempo_total>"
  curl -s -o /dev/null -w "%{http_code} %{time_total}\n" \
    -X POST "http://localhost:$WORKER_PORT/transcribe/url" \
    -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
    -d "{\"url\":\"$AUDIO_URL\",\"language\":\"pt\"}"
}
export -f send_one
export WORKER_PORT API_KEY AUDIO_URL

T0=$(date +%s.%N)
seq "$REQUESTS" | xargs -P "$SENDERS" -I{} bash -c 'send_one' > "$TMP/raw.txt"
T1=$(date +%s.%N)
awk '{print $1}' "$TMP/raw.txt" > "$CODES"
awk '{print $2}' "$TMP/raw.txt" > "$LAT"

SEND_SECS=$(awk "BEGIN{printf \"%.2f\", $T1-$T0}")
N202=$(grep -c '^202' "$CODES" || true)
N429=$(grep -c '^429' "$CODES" || true)
N5XX=$(grep -c '^5' "$CODES" || true)
NOTH=$(grep -vcE '^(202|429)' "$CODES" || true)
RPS=$(awk "BEGIN{printf \"%.0f\", $REQUESTS/$SEND_SECS}")

# latências (ms) ordenadas para p50/p95/p99
sort -n "$LAT" > "$TMP/lat_sorted.txt"
pct() { awk -v p="$1" 'NR==FNR{a[NR]=$1;n=NR;next}END{i=int(p/100*n); if(i<1)i=1; printf "%.0f", a[i]*1000}' "$TMP/lat_sorted.txt" "$TMP/lat_sorted.txt"; }
P50=$(pct 50); P95=$(pct 95); P99=$(pct 99)

echo "  ${c_green}intake:${c_off} ${SEND_SECS}s  (~${RPS} req/s)"
echo "  202 aceitos: $N202   429 backpressure: $N429   5xx: $N5XX   outros: $NOTH"
echo "  latência intake  p50=${P50}ms  p95=${P95}ms  p99=${P99}ms"

# =====================================================================
info "5/5  drenando a fila e medindo conclusão"
PEAK_HEAP=0; PEAK_INFLIGHT=0
DEADLINE=$(( SECONDS + 180 ))
while :; do
  H=$(curl -s "http://localhost:$WORKER_PORT/health")
  Q=$(echo "$H" | sed -E 's/.*"queueDepth":([0-9]+).*/\1/')
  INF=$(echo "$H" | sed -E 's/.*"inFlight":([0-9]+).*/\1/')
  HEAP=$(echo "$H" | sed -E 's/.*"heapAllocMB":([0-9]+).*/\1/')
  AUD=$(echo "$H" | sed -E 's/.*"audioInMemory":([0-9]+).*/\1/')
  (( HEAP > PEAK_HEAP )) && PEAK_HEAP=$HEAP
  (( INF > PEAK_INFLIGHT )) && PEAK_INFLIGHT=$INF
  printf "\r  fila=%-5s inFlight=%-3s heapMB=%-4s audioRAM=%-4s   " "$Q" "$INF" "$HEAP" "$AUD"
  [[ "$Q" == "0" && "$INF" == "0" ]] && break
  (( SECONDS >= DEADLINE )) && { echo; warn "deadline de drenagem atingido"; break; }
  sleep 0.5
done
echo
T2=$(date +%s.%N)
TOTAL_SECS=$(awk "BEGIN{printf \"%.2f\", $T2-$T0}")

# Contabiliza completed/failed amostrando os logs do worker.
COMPLETED=$(grep -c "job completed" "$TMP/worker.log" || true)
FAILED=$(grep -c "job failed" "$TMP/worker.log" || true)
THRU=$(awk "BEGIN{printf \"%.1f\", $COMPLETED/$TOTAL_SECS}")

echo
echo "================  RESULTADO  ================"
echo "  requisições:        $REQUESTS (concorrência $SENDERS)"
echo "  aceitas (202):      $N202"
echo "  backpressure (429): $N429"
echo "  erros (5xx/outros): $((N5XX+NOTH))"
echo "  intake:             ${SEND_SECS}s (~${RPS} req/s)  p50=${P50}ms p95=${P95}ms p99=${P99}ms"
echo "  jobs completed:     $COMPLETED"
echo "  jobs failed:        $FAILED"
echo "  tempo total:        ${TOTAL_SECS}s"
echo "  throughput:         ${THRU} jobs/s (MAX_CONCURRENCY=$MAX_CONCURRENCY, Whisper=${WHISPER_MS}ms)"
echo "  pico inFlight:      $PEAK_INFLIGHT (teto=$MAX_CONCURRENCY)"
echo "  pico heap:          ${PEAK_HEAP}MB"
echo "  áudio em RAM final: ${AUD}  (esperado 0)"
echo
if [[ "${AUD:-0}" == "0" ]]; then ok "áudio totalmente liberado da memória"; else warn "áudio ainda em memória: $AUD"; fi
if (( N5XX+NOTH > 0 )); then warn "houve erros não-backpressure; ver $TMP/worker.log"; tail -10 "$TMP/worker.log"; fi
