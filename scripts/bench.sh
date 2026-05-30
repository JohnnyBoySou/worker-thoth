#!/usr/bin/env bash
#
# Benchmark de capacidade do worker-thoth: mede req/s reais por caminho de
# código (rejeição 429, enfileiramento 202, leitura) e throughput de jobs por
# MAX_CONCURRENCY. Usa um gerador de carga em Go (keep-alive + pool) para não
# saturar no gerador. Self-contained: Redis (docker) + Whisper falso + áudio
# local real. NÃO toca produção.
#
# Uso: ./scripts/bench.sh
# Requisitos: go, docker, curl, python3.

set -euo pipefail

REDIS_PORT="${REDIS_PORT:-6393}"
MOCK_PORT="${MOCK_PORT:-8796}"
AUDIO_PORT="${AUDIO_PORT:-8795}"
WORKER_PORT="${WORKER_PORT:-8773}"
API_KEY="${API_KEY:-bench-key}"
REDIS_NAME="thoth-bench-redis"
AUDIO_URL_WEB="${AUDIO_URL_WEB:-https://download.samplelib.com/mp3/sample-15s.mp3}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
BIN="$TMP/worker"; GEN="$TMP/gen"

c_blue=$'\033[0;34m'; c_green=$'\033[0;32m'; c_off=$'\033[0m'
info() { echo "${c_blue}==>${c_off} $*"; }

cleanup() { set +e
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null
  [[ -n "${MOCK_PID:-}" ]]   && kill "$MOCK_PID" 2>/dev/null
  [[ -n "${AUDIO_PID:-}" ]]  && kill "$AUDIO_PID" 2>/dev/null
  docker rm -f "$REDIS_NAME" >/dev/null 2>&1
  rm -rf "$TMP"
}
trap cleanup EXIT
wait_http() { local url="$1" deadline=$(( SECONDS + ${2:-15} ))
  until curl -sf -o /dev/null "$url" 2>/dev/null; do (( SECONDS >= deadline )) && return 1; sleep 0.3; done; }

# --------- gerador de carga em Go (stdlib, keep-alive, concorrência) ---------
mkdir -p "$TMP/gen_src"
cat > "$TMP/gen_src/go.mod" <<'GOMOD'
module loadgen
go 1.21
GOMOD
cat > "$TMP/gen_src/main.go" <<'GO'
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func env(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

func main() {
	url := os.Getenv("U")
	method := env("M", "GET")
	body := os.Getenv("B")
	key := os.Getenv("K")
	total := atoi(env("N", "20000"))
	conc := atoi(env("C", "200"))

	bodyBytes := []byte(body)
	tr := &http.Transport{MaxIdleConns: conc * 2, MaxIdleConnsPerHost: conc * 2, IdleConnTimeout: 30 * time.Second}
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}

	jobs := make(chan int, total)
	for i := 0; i < total; i++ { jobs <- i }
	close(jobs)

	lat := make([]int64, total) // ns
	var c2xx, c429, cOther, cErr int64
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				var rdr io.Reader
				if len(bodyBytes) > 0 { rdr = bytes.NewReader(bodyBytes) }
				req, _ := http.NewRequest(method, url, rdr)
				if key != "" { req.Header.Set("X-API-Key", key) }
				if len(bodyBytes) > 0 { req.Header.Set("Content-Type", "application/json") }
				t0 := time.Now()
				resp, err := client.Do(req)
				lat[i] = time.Since(t0).Nanoseconds()
				if err != nil { atomic.AddInt64(&cErr, 1); continue }
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				switch {
				case resp.StatusCode >= 200 && resp.StatusCode < 300:
					atomic.AddInt64(&c2xx, 1)
				case resp.StatusCode == 429:
					atomic.AddInt64(&c429, 1)
				default:
					atomic.AddInt64(&cOther, 1)
				}
			}
		}()
	}
	wg.Wait()
	dur := time.Since(start)

	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	pct := func(p float64) float64 {
		i := int(p / 100 * float64(total))
		if i >= total { i = total - 1 }
		if i < 0 { i = 0 }
		return float64(lat[i]) / 1e6 // ms
	}
	rps := float64(total) / dur.Seconds()
	fmt.Printf("RPS=%.0f DUR=%.2f P50=%.2f P95=%.2f P99=%.2f C2XX=%d C429=%d COTHER=%d CERR=%d\n",
		rps, dur.Seconds(), pct(50), pct(95), pct(99), c2xx, c429, cOther, cErr)
}
GO
( cd "$TMP/gen_src" && go build -o "$GEN" . )

# --------------------------- sobe a stack ---------------------------
info "build worker + gerador, baixando áudio real e subindo deps"
cd "$ROOT"
go build -o "$BIN" ./cmd/server
curl -sf -L "$AUDIO_URL_WEB" -o "$TMP/audio.mp3"
docker rm -f "$REDIS_NAME" >/dev/null 2>&1 || true
docker run -d --name "$REDIS_NAME" -p "$REDIS_PORT:6379" redis:7-alpine >/dev/null

cat > "$TMP/mock.py" <<PY
import http.server, json, time, socketserver
DELAY = ${WHISPER_MS:-100} / 1000.0
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length",0))); time.sleep(DELAY)
        b=json.dumps({"text":"x","language":"pt","elapsed_ms":1}).encode()
        self.send_response(200); self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*a): pass
class S(socketserver.ThreadingMixIn, http.server.HTTPServer): daemon_threads=True
S(("127.0.0.1",$MOCK_PORT),H).serve_forever()
PY
python3 "$TMP/mock.py" & MOCK_PID=$!
cat > "$TMP/audiosrv.py" <<PY
import http.server, socketserver
DATA=open("$TMP/audio.mp3","rb").read()
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header("Content-Length",str(len(DATA))); self.end_headers(); self.wfile.write(DATA)
    def log_message(self,*a): pass
class S(socketserver.ThreadingMixIn, http.server.HTTPServer): daemon_threads=True
S(("127.0.0.1",$AUDIO_PORT),H).serve_forever()
PY
python3 "$TMP/audiosrv.py" & AUDIO_PID=$!
sleep 0.5

start_worker() { # start_worker <max_concurrency> <queue_size>
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null; sleep 0.3
  API_KEY="$API_KEY" WHISPER_API_KEY="up" REDIS_URL="redis://localhost:$REDIS_PORT/0" \
    WHISPER_UPSTREAM_URL="http://127.0.0.1:$MOCK_PORT" PORT="$WORKER_PORT" \
    MAX_CONCURRENCY="$1" QUEUE_SIZE="$2" JOB_TTL=30m \
    "$BIN" > "$TMP/worker.log" 2>&1 & WORKER_PID=$!
  wait_http "http://localhost:$WORKER_PORT/health" 15
}
AUDIO_URL="http://127.0.0.1:$AUDIO_PORT/clip.mp3"
BODY="{\"url\":\"$AUDIO_URL\",\"language\":\"pt\"}"

# ============================ TABELA 1: intake req/s por caminho ============================
echo
echo "================  TABELA 1 — capacidade de intake HTTP (req/s)  ================"
printf "%-34s %8s %7s %7s %7s  %s\n" "cenário (caminho)" "req/s" "p50ms" "p95ms" "p99ms" "status"

# A) leitura: GET /health (sem auth, sem Redis)
start_worker 8 100
R=$(U="http://localhost:$WORKER_PORT/health" M=GET N=40000 C=200 "$GEN")
eval "$R"; printf "%-34s %8s %7s %7s %7s  %s\n" "GET /health (leitura)" "$RPS" "$P50" "$P95" "$P99" "200:$C2XX"

# B) rejeição 401: POST sem key
R=$(U="http://localhost:$WORKER_PORT/transcribe/url" M=POST B="$BODY" N=40000 C=200 "$GEN")
eval "$R"; printf "%-34s %8s %7s %7s %7s  %s\n" "POST /url sem auth (401)" "$RPS" "$P50" "$P95" "$P99" "401:$COTHER"

# C) backpressure 429: fila=1 + whisper lento => quase tudo rejeita na fila cheia
start_worker 1 1
R=$(U="http://localhost:$WORKER_PORT/transcribe/url" M=POST B="$BODY" K="$API_KEY" N=40000 C=200 "$GEN")
eval "$R"; printf "%-34s %8s %7s %7s %7s  %s\n" "POST /url fila cheia (429)" "$RPS" "$P50" "$P95" "$P99" "429:$C429 202:$C2XX"

# D) enfileiramento 202: fila enorme => mede accept+Redis XADD por request
start_worker 16 60000
R=$(U="http://localhost:$WORKER_PORT/transcribe/url" M=POST B="$BODY" K="$API_KEY" N=40000 C=200 "$GEN")
eval "$R"; printf "%-34s %8s %7s %7s %7s  %s\n" "POST /url enfileira (202+Redis)" "$RPS" "$P50" "$P95" "$P99" "202:$C2XX 429:$C429"
echo "  (todos os 40k jobs aceitos vão ser processados; ignore a fila residual ao encerrar)"

echo
echo "Observações:"
echo "  - 'leitura' e '401'/'429' não tocam o Redis -> teto puro de HTTP do worker."
echo "  - '202' inclui um XADD no Redis por request -> custo real de enfileirar."
