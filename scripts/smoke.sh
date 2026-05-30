#!/usr/bin/env bash
#
# Smoke test self-contained do worker-thoth.
#
# Sobe um Redis (docker) e um Whisper FALSO (que também serve um "áudio"),
# compila e roda o worker, e exercita todas as rotas validando os resultados.
# Não precisa de rede externa nem do Whisper real.
#
# Uso:   ./scripts/smoke.sh
# Saída: 0 se tudo passar; !=0 no primeiro erro.
#
# Requisitos: go, docker, curl, python3.

set -euo pipefail

# --- parâmetros (ajustáveis por env) ---
REDIS_PORT="${REDIS_PORT:-6391}"
MOCK_PORT="${MOCK_PORT:-8799}"
WORKER_PORT="${WORKER_PORT:-8771}"
API_KEY="${API_KEY:-cli-test-key}"
REDIS_NAME="thoth-smoke-redis"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
BIN="$TMP/worker"
PASS=0
FAIL=0

# --- helpers de log/assert ---
c_green=$'\033[0;32m'; c_red=$'\033[0;31m'; c_blue=$'\033[0;34m'; c_off=$'\033[0m'
info() { echo "${c_blue}==>${c_off} $*"; }
ok()   { echo "  ${c_green}PASS${c_off} $*"; PASS=$((PASS+1)); }
bad()  { echo "  ${c_red}FAIL${c_off} $*"; FAIL=$((FAIL+1)); }

assert_eq() { # assert_eq <descrição> <esperado> <obtido>
  if [[ "$2" == "$3" ]]; then ok "$1 ($3)"; else bad "$1: esperado '$2', obtido '$3'"; fi
}
assert_contains() { # assert_contains <descrição> <agulha> <palheiro>
  if [[ "$3" == *"$2"* ]]; then ok "$1"; else bad "$1: '$2' não encontrado em '$3'"; fi
}

# --- limpeza garantida ---
cleanup() {
  set +e
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null
  [[ -n "${MOCK_PID:-}" ]] && kill "$MOCK_PID" 2>/dev/null
  docker rm -f "$REDIS_NAME" >/dev/null 2>&1
  rm -rf "$TMP"
}
trap cleanup EXIT

# --- espera até uma URL responder ---
wait_http() { # wait_http <url> <segundos>
  local url="$1" deadline=$(( SECONDS + ${2:-15} ))
  until curl -sf -o /dev/null "$url" 2>/dev/null; do
    (( SECONDS >= deadline )) && { echo "timeout esperando $url"; return 1; }
    sleep 0.3
  done
}

# --- poll até o job sair de queued/processing ---
poll_status() { # poll_status <jobId> -> imprime status final
  local id="$1" deadline=$(( SECONDS + 20 )) body status
  while :; do
    body=$(curl -s "http://localhost:$WORKER_PORT/jobs/$id" -H "X-API-Key: $API_KEY")
    status=$(echo "$body" | sed -E 's/.*"status":"([^"]+)".*/\1/')
    [[ "$status" == "completed" || "$status" == "failed" ]] && { echo "$body"; return 0; }
    (( SECONDS >= deadline )) && { echo "$body"; return 1; }
    sleep 0.3
  done
}

# =====================================================================
info "1/6  build (go test + go build)"
cd "$ROOT"
go test ./... >/dev/null && ok "go test"
go build -o "$BIN" ./cmd/server && ok "go build"

# =====================================================================
info "2/6  subindo dependências (Redis + Whisper falso)"
docker rm -f "$REDIS_NAME" >/dev/null 2>&1 || true
docker run -d --name "$REDIS_NAME" -p "$REDIS_PORT:6379" redis:7-alpine >/dev/null
ok "redis em :$REDIS_PORT"

cat > "$TMP/mock.py" <<PY
import http.server, json
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # serve um "áudio" qualquer (para o job por URL)
        self.send_response(200); self.send_header("Content-Length","5"); self.end_headers()
        self.wfile.write(b"AUDIO")
    def do_POST(self):  # finge ser o Whisper /transcribe
        self.rfile.read(int(self.headers.get("Content-Length",0)))
        b=json.dumps({"text":"olá mundo","language":"pt","elapsed_ms":42}).encode()
        self.send_response(200); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*a): pass
http.server.HTTPServer(("127.0.0.1",$MOCK_PORT),H).serve_forever()
PY
python3 "$TMP/mock.py" & MOCK_PID=$!
wait_http "http://127.0.0.1:$MOCK_PORT/" 10 && ok "whisper falso em :$MOCK_PORT"

# =====================================================================
info "3/6  iniciando worker"
API_KEY="$API_KEY" WHISPER_API_KEY="up" \
  REDIS_URL="redis://localhost:$REDIS_PORT/0" \
  WHISPER_UPSTREAM_URL="http://127.0.0.1:$MOCK_PORT" \
  PORT="$WORKER_PORT" MAX_CONCURRENCY=2 JOB_TTL=10m \
  "$BIN" > "$TMP/worker.log" 2>&1 & WORKER_PID=$!
wait_http "http://localhost:$WORKER_PORT/health" 15 && ok "worker em :$WORKER_PORT"

# =====================================================================
info "4/6  rotas básicas (health / auth / 404)"
HEALTH=$(curl -s "http://localhost:$WORKER_PORT/health")
assert_contains "/health status ok" '"status":"ok"' "$HEALTH"
assert_contains "/health redisOk"   '"redisOk":true' "$HEALTH"

CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:$WORKER_PORT/transcribe/url" -d '{}')
assert_eq "POST sem X-API-Key -> 401" "401" "$CODE"

CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$WORKER_PORT/jobs/nao-existe" -H "X-API-Key: $API_KEY")
assert_eq "GET job inexistente -> 404" "404" "$CODE"

# auth via Authorization: Bearer (compat com gateway) deve passar; 404 = autenticou.
CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$WORKER_PORT/jobs/nao-existe" -H "Authorization: Bearer $API_KEY")
assert_eq "auth via Bearer aceita (404, não 401)" "404" "$CODE"

# =====================================================================
info "5/6  job por URL"
RESP=$(curl -s -X POST "http://localhost:$WORKER_PORT/transcribe/url" \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d "{\"url\":\"http://127.0.0.1:$MOCK_PORT/clip.mp3\",\"language\":\"pt\"}")
assert_contains "URL -> 202 queued" '"status":"queued"' "$RESP"
JOB=$(echo "$RESP" | sed -E 's/.*"jobId":"([^"]+)".*/\1/')
FINAL=$(poll_status "$JOB")
assert_contains "URL job completed" '"status":"completed"' "$FINAL"
assert_contains "URL repassa corpo do Whisper" '"elapsed_ms":42' "$FINAL"

# alias do gateway (/v1/audio/transcriptions/...) + auth via Bearer
GW="http://localhost:$WORKER_PORT/v1/audio/transcriptions"
RESP=$(curl -s -X POST "$GW/url" \
  -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" \
  -d "{\"url\":\"http://127.0.0.1:$MOCK_PORT/clip.mp3\"}")
assert_contains "alias gateway POST /url -> queued" '"status":"queued"' "$RESP"
JOB=$(echo "$RESP" | sed -E 's/.*"jobId":"([^"]+)".*/\1/')
FINAL=$(curl -s "$GW/jobs/$JOB" -H "Authorization: Bearer $API_KEY")
# pode ainda estar processando; só garante que a rota existe (não 404 de rota)
assert_contains "alias gateway GET /jobs aceito" "$JOB" "$FINAL"

# =====================================================================
info "6/6  job por upload (áudio efêmero)"
head -c 2048 /dev/urandom > "$TMP/clip.wav"
RESP=$(curl -s -X POST "http://localhost:$WORKER_PORT/transcribe" \
  -H "X-API-Key: $API_KEY" -F "audio=@$TMP/clip.wav" -F "language=pt")
assert_contains "upload -> 202 queued" '"status":"queued"' "$RESP"
JOB=$(echo "$RESP" | sed -E 's/.*"jobId":"([^"]+)".*/\1/')
FINAL=$(poll_status "$JOB")
assert_contains "upload job completed" '"status":"completed"' "$FINAL"
HEALTH=$(curl -s "http://localhost:$WORKER_PORT/health")
assert_contains "áudio liberado da memória" '"audioInMemory":0' "$HEALTH"

# =====================================================================
echo
echo "================  resultado  ================"
echo "  ${c_green}PASS: $PASS${c_off}   ${c_red}FAIL: $FAIL${c_off}"
if (( FAIL > 0 )); then
  echo "  log do worker:"; sed 's/^/    /' "$TMP/worker.log" | tail -20
  exit 1
fi
echo "  ${c_green}tudo ok${c_off}"
