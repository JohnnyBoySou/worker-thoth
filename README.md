# worker-transcription

Worker **assíncrono** em Go para transcrição de áudio. Recebe a requisição
(upload ou URL), **enfileira no Redis**, consome com um pool de workers e repassa
ao Whisper hospedado em `WHISPER_UPSTREAM_URL`. O **áudio é efêmero**: vive só em
memória durante o processamento e é liberado ao concluir — apenas o **texto**
resultante fica no Redis, com TTL.

Implementa a spec de `../TRANSCRIPTION.md` §2.

## Arquitetura

```
Cliente ──POST (X-API-Key)──► HTTP handler ──enfileira jobId──► Redis Stream
   │ 202 {jobId}                                                     │
   │                                          pool de N workers ◄─────┘
   │                                                 │ baixa/recebe áudio (RAM)
   │                                                 ▼ POST /transcribe (X-API-Key)
   │                                          whisper.lai.ia.br
   │                                                 │ grava texto no Redis (TTL)
   └──GET /jobs/:id──► lê status/result ◄────────────┘  e libera o áudio
```

- **Fila + estado:** Redis Stream (`transcription:queue`) com consumer group
  `workers` para entrega *at-least-once* (`XACK` + `XAUTOCLAIM` para órfãos).
  Estado por job no hash `job:{id}` com TTL (`JOB_TTL`).
- **Concorrência:** `MAX_CONCURRENCY` goroutines processam jobs em paralelo
  (downloads de URL, preparo do multipart). A chamada ao Whisper, porém, é
  **serializada por um gate interno no client** → no máximo **uma** transcrição
  bate na GPU única por vez, independentemente de `MAX_CONCURRENCY`.
- **Memória:** uploads ficam no store em memória do processo (`internal/audio`),
  removidos e zerados ao concluir. Downloads por URL respeitam `MAX_AUDIO_BYTES`
  via `Content-Length` **e** `io.LimitReader` (não confia no header).

> **Constraint de deploy:** como o áudio de upload vive na RAM do processo (nunca
> em disco/Redis), os jobs de **upload** devem ser processados pela mesma
> instância que os aceitou — rode **uma instância** ou use sticky routing. Jobs
> por **URL** são independentes da instância. Se um upload órfão for reivindicado
> após restart, ele falha com erro claro (o áudio não é recuperável, por design).

## API HTTP

| Método/Rota            | Auth   | Descrição |
|------------------------|--------|-----------|
| `GET /health`          | livre  | status, profundidade da fila, jobs em voo, áudio em RAM, heap |
| `POST /transcribe`     | sim    | `multipart`: `audio` (File), `language` (opcional) → `202 {jobId,status}` |
| `POST /transcribe/url` | sim    | JSON `{ url, language }` → `202 {jobId,status}` |
| `GET /jobs/:jobId`     | sim    | `{ jobId, status, result, error, createdAt, completedAt }` |

- **Auth:** envie a chave via `X-API-Key: <key>` **ou** `Authorization: Bearer <key>`
  (compat. com clientes estilo gateway). `X-API-Key` tem precedência se ambos vierem.
  Comparação constant-time; sem chave válida → `401`.
- **Aliases de gateway:** as rotas de transcrição também respondem sob o prefixo
  `/v1/audio/transcriptions` — i.e. `POST /v1/audio/transcriptions`,
  `POST /v1/audio/transcriptions/url` e `GET /v1/audio/transcriptions/jobs/:id` —
  para clientes que já chamam `gateway.lai.ia.br/v1/audio/transcriptions/...`.
- `status`: `queued` → `processing` → `completed` \| `failed`.
- Concluído: `result` = corpo do Whisper **intacto** (`{text,language,elapsed_ms}`).
- `404` se o `jobId` não existe **ou expirou** (TTL).
- `429` + `Retry-After` quando a fila atinge `QUEUE_SIZE`.

### Exemplos

```bash
# upload
curl -s -X POST http://localhost:8770/transcribe \
  -H "X-API-Key: $API_KEY" \
  -F "audio=@amostra.mp3" -F "language=pt"
# {"jobId":"...","status":"queued"}

# por URL
curl -s -X POST http://localhost:8770/transcribe/url \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"url":"https://exemplo.com/audio.mp3","language":"pt"}'

# polling
curl -s http://localhost:8770/jobs/<jobId> -H "X-API-Key: $API_KEY"
```

## Configuração

Ver `.env.example`. Variáveis principais: `API_KEY`, `WHISPER_API_KEY` (segredos,
obrigatórios), `REDIS_URL`, `WHISPER_UPSTREAM_URL`, `MAX_CONCURRENCY`,
`QUEUE_SIZE`, `JOB_TTL`, `MAX_AUDIO_BYTES`, `DOWNLOAD_TIMEOUT`,
`UPSTREAM_TIMEOUT`, `DEFAULT_LANGUAGE`.

## Rodando

```bash
# local (precisa de um Redis acessível)
cp .env.example .env   # preencha os segredos
export $(grep -v '^#' .env | xargs)
go run ./cmd/server

# docker compose (sobe Redis + worker)
API_KEY=... WHISPER_API_KEY=... docker compose up --build
```

## Testes

```bash
go test ./...   # unitários
go vet ./...
```

Cobrem: store de áudio (Put/Take/Drop + zeragem), parsing de config, cliente
Whisper (forward de `X-API-Key`/`language`/áudio, 4xx permanente vs 5xx
transitório) e download (limite por `Content-Length` e por stream chunked).

### Smoke test end-to-end

`scripts/smoke.sh` é self-contained: sobe um Redis (docker) e um Whisper **falso**,
compila e roda o worker e exercita todas as rotas (`/health`, 401, 404, job por
URL e por upload), validando que o job chega a `completed`, que o corpo do
Whisper é repassado e que o áudio é liberado da memória (`audioInMemory:0`).

```bash
./scripts/smoke.sh
# Requisitos: go, docker, curl, python3. Não precisa do Whisper real.
```

## Layout

```
cmd/server/main.go          # wiring + graceful shutdown
internal/config/            # env -> Config (fail-fast)
internal/server/            # HTTP: auth X-API-Key, handlers, health
internal/redisstore/        # fila (Stream + group) + estado job:{id} com TTL
internal/worker/            # pool de workers, download por URL, retry
internal/whisper/           # cliente HTTP para o Whisper hospedado
internal/audio/             # store efêmero de áudio em memória
internal/job/               # modelo de domínio do job
```
