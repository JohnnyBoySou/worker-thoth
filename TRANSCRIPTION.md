# Sistema de Transcrição (Adila Ops)

Transcrição de áudio → texto via um **worker dedicado em Go** que recebe a requisição,
**enfileira no Redis**, consome de forma assíncrona e repassa para o Whisper hospedado
em **`https://whisper.lai.ia.br`** usando uma API key.

> **Resumo:** o worker Go é o serviço completo de transcrição — ponto de entrada HTTP,
> fila Redis, controle de concorrência, acompanhamento de status (também no Redis) e
> repasse ao Whisper. O cliente fala direto com ele. **O áudio é efêmero**: vive em
> memória só durante o processamento e é **liberado assim que a transcrição termina**;
> apenas o texto resultante é guardado (com TTL).

---

## Índice

1. [Arquitetura](#1-arquitetura)
2. [Worker de transcrição em Go (spec)](#2-worker-de-transcrição-em-go-spec) ← **alvo da implementação**
3. [Motor de transcrição — Whisper](#3-motor-de-transcrição--whisper)
4. [Configuração e segredos](#4-configuração-e-segredos)
5. [Mapa de arquivos](#5-mapa-de-arquivos)

---

## 1. Arquitetura

O worker Go é o **serviço completo de transcrição**, **assíncrono**, com **Redis** como
fila e store de estado. O cliente fala direto com ele.

```
  Cliente / Dashboard
        │  ① POST áudio (multipart) ou URL  +  X-API-Key
        ▼
  ┌──────────────────────────────────────────────────────┐
  │                worker-go (transcrição)                 │
  │                                                        │
  │  handler HTTP ──► enfileira jobId ──►  ┌────────────┐  │
  │       │ 202 {jobId}                    │   Redis    │  │
  │       │                                │  fila +    │  │
  │       │           ┌─────────────────── │  estado    │  │
  │       │           │  N workers          └────────────┘  │
  │       │           ▼  (goroutines)            ▲          │
  │       │   baixa/recebe áudio (memória)       │ status   │
  │       │           │                          │ +result  │
  │       │           ▼  POST /transcribe        │ (TTL)    │
  │       │   ┌──────────────────────────┐       │          │
  │       │   │ https://whisper.lai.ia.br│───────┘          │
  │       │   └──────────────────────────┘                  │
  │       │           │ ao concluir: grava texto no Redis    │
  │       │           ▼ e **libera o áudio da memória**       │
  │  ② GET /jobs/:id ─► lê status/result do Redis             │
  └──────────────────────────────────────────────────────┘
```

Responsabilidades do worker Go:

- Ponto de entrada HTTP: **upload** (multipart) e **URL** (baixa o áudio).
- Autenticação do cliente (API key).
- **Fila Redis** + controle de concorrência rumo ao Whisper (GPU única).
- Acompanhamento de status e guarda do **resultado (texto)** no Redis, com TTL.
- Repasse ao Whisper hospedado com a `X-API-Key` do upstream.
- **Gestão de memória:** o áudio (upload recebido ou baixado da URL) fica só em memória
  durante o processamento e é **descartado imediatamente após a transcrição** —
  sucesso ou falha — para não estourar o sistema. **Nunca persiste áudio.**

### Decisões fechadas

| Decisão              | Escolha                                                              |
|----------------------|---------------------------------------------------------------------|
| Contrato com cliente | **Assíncrono** — `POST` devolve `202 + jobId`; cliente faz polling   |
| Fila + estado        | **Redis** (fila de jobs + status/result com TTL)                    |
| Entrada por URL      | **Sim** — worker baixa o áudio, transcreve, e libera                 |
| Áudio                | **Efêmero em memória**, liberado ao concluir; nunca persistido       |
| Resultado            | Texto guardado no Redis com **TTL** (expira sozinho)                 |

> ⚠️ **Três "workers" distintos — não confundir:**
> - `worker/` (Go, já existe) = **treinamento** (datasets, GPU, vector store). Nada de transcrição.
> - `worker-whisper/` (Python/FastAPI) = **motor** Whisper. `whisper.lai.ia.br` é uma instância hospedada dele.
> - **novo worker Go** (sugestão: `worker-transcription/`) = **fila + repasse** ao Whisper. É o que esta migração cria.

---

## 2. Worker de transcrição em Go (spec)

Serviço Go **assíncrono** a ser criado. Recebe transcrições (upload ou URL),
**enfileira no Redis**, consome e repassa para `https://whisper.lai.ia.br`, guarda o
texto no Redis com TTL e **libera o áudio da memória** ao terminar.

### 2.1. Contrato HTTP

| Método/Rota            | Auth        | Descrição                                                   |
|------------------------|-------------|-------------------------------------------------------------|
| `GET /health`          | livre       | liveness + métricas da fila (tamanho, jobs em voo, upstream)|
| `POST /transcribe`     | `X-API-Key` | `multipart` (`audio`, `language`); enfileira → **202** `{ jobId, status: "queued" }` |
| `POST /transcribe/url` | `X-API-Key` | JSON `{ url, language }`; enfileira → **202** `{ jobId, status: "queued" }` |
| `GET /jobs/:jobId`      | `X-API-Key` | `{ jobId, status, result, error, createdAt, completedAt }`  |

- **Resposta de `GET /jobs/:id`** (após concluído): `result` = corpo do Whisper intacto,
  ex.: `{ "text": "...", "language": "pt", "elapsed_ms": 320 }`. Em falha, `error` preenchido.
- **404** se o `jobId` não existe **ou já expirou** (TTL). Documentar isso para o cliente.
- Campos: `audio` (File, obrigatório no upload), `url` (string, obrigatório no `/url`),
  `language` (default `pt`, ISO 639-1).

### 2.2. Fila e estado no Redis

- **Fila:** Redis Stream (recomendado, com consumer group p/ ack e reprocesso) ou Lista
  (`LPUSH`/`BRPOP`). O `POST` cria o registro do job + enfileira o `jobId`.
- **Estado do job:** chave `job:{id}` (hash) com `status`, `result`, `error`,
  `createdAt`, `completedAt`, `language`. **TTL** aplicado na chave (resultado expira
  sozinho — ver `JOB_TTL`).
- **Status:** `queued` → `processing` → `completed` | `failed`.
- **Backpressure:** se a fila atingir `QUEUE_SIZE`, o `POST` responde **429** (ou **503**
  + `Retry-After`) em vez de aceitar indefinidamente.
- **Idempotência/at-least-once:** com Stream + consumer group, usar `XACK` só após
  concluir; jobs órfãos (worker morto) podem ser reivindicados (`XAUTOCLAIM`).

### 2.3. Workers (consumidores) e concorrência

- **N workers** (goroutines) consomem a fila → no máximo N chamadas simultâneas ao
  Whisper (proteger a GPU única). Começar com `MAX_CONCURRENCY` pequeno (1–2).
- Fluxo de cada job:
  1. Marca `status: processing` no Redis.
  2. **Obtém o áudio em memória:** upload já recebido, ou **baixa da URL** (timeout
     `DOWNLOAD_TIMEOUT`; validar `Content-Length` contra `MAX_AUDIO_BYTES`).
  3. `POST {WHISPER_UPSTREAM_URL}/transcribe` (multipart `audio`+`language`) com
     `X-API-Key: {WHISPER_API_KEY}` e `UPSTREAM_TIMEOUT`.
  4. Grava `result` (corpo do Whisper) e `status: completed` no Redis (com TTL); em erro,
     `status: failed` + `error`.
  5. **Libera o buffer do áudio** (sucesso ou falha) — sem reter referência; deixar o GC
     recolher. Em Go: zerar o slice/ponteiro após o passo 4.
- **Retry** opcional para falhas transitórias (5xx/timeout) com backoff e teto; **não**
  reenviar em 4xx.

### 2.4. Gestão de memória (requisito explícito)

- Áudio (upload ou baixado) vive **apenas durante o processamento** e é **liberado ao
  concluir** — para não estourar a RAM. **Nunca é persistido em disco** nem no Redis.
- Limites: `MAX_AUDIO_BYTES` (rejeitar uploads/downloads maiores que X) e
  `MAX_CONCURRENCY` limitam o áudio simultâneo em memória ≈ `MAX_AUDIO_BYTES × N`.
- O que **fica** no Redis é só o **texto** resultante, e com **TTL** (`JOB_TTL`).
- Opcional (se RAM for crítica): em vez de `[]byte`, fazer streaming do download direto
  para o request multipart ao Whisper, sem materializar tudo na memória.

### 2.5. Configuração (env)

| Var                    | Exemplo                       | Descrição                                       |
|------------------------|-------------------------------|-------------------------------------------------|
| `PORT`                 | `8770`                        | porta HTTP do worker Go                         |
| `API_KEY`              | `<segredo>`                   | autentica cliente → worker Go (`X-API-Key`)     |
| `REDIS_URL`            | `redis://localhost:6379/0`    | conexão Redis (fila + estado)                   |
| `WHISPER_UPSTREAM_URL` | `https://whisper.lai.ia.br`   | endpoint do Whisper hospedado                   |
| `WHISPER_API_KEY`      | `<segredo>`                   | API key enviada ao Whisper (`X-API-Key`)        |
| `MAX_CONCURRENCY`      | `2`                           | goroutines de processamento (download/preparo); envio ao Whisper é serializado (1 por vez) |
| `QUEUE_SIZE`           | `5000`                        | profundidade máx. da fila antes de 429          |
| `JOB_TTL`              | `1h`                          | TTL do resultado/estado no Redis                |
| `MAX_AUDIO_BYTES`      | `104857600` (100 MB)          | tamanho máx. de upload/download                 |
| `DOWNLOAD_TIMEOUT`     | `600s`                        | timeout do download por URL                     |
| `UPSTREAM_TIMEOUT`     | `600s`                        | timeout da chamada ao Whisper                   |
| `DEFAULT_LANGUAGE`     | `pt`                          | idioma default quando não informado             |
| `WHISPER_WINDOW_START` | `""` (vazio = desabilitado)   | início da janela de envio ao Whisper (`HH:MM`)  |
| `WHISPER_WINDOW_END`   | `""`                          | fim da janela (`HH:MM`; pode cruzar meia-noite) |
| `WHISPER_WINDOW_TZ`    | `America/Sao_Paulo`           | timezone IANA para interpretar START/END        |

> **Segredos:** `API_KEY` e `WHISPER_API_KEY` em env/secret manager, **nunca** no código.

### 2.6. Observabilidade (recomendado)

- `GET /health`: tamanho da fila, jobs em voo, disponibilidade do upstream, uso de RAM.
- Logs por job: espera na fila, duração do download, duração no Whisper, status,
  tamanho do áudio.
- Métricas: profundidade da fila, rejeições (429), latência p50/p95, taxa de erro,
  jobs expirados por TTL.

### 2.7. Checklist de implementação

- [ ] Servidor HTTP Go: `GET /health`, `POST /transcribe`, `POST /transcribe/url`, `GET /jobs/:id`.
- [ ] Middleware de auth `X-API-Key` (comparar com `API_KEY`).
- [ ] Cliente Redis: fila (Stream/Lista) + estado `job:{id}` com TTL (`JOB_TTL`).
- [ ] Pool de N workers (`MAX_CONCURRENCY`) consumindo a fila.
- [ ] Download por URL com `DOWNLOAD_TIMEOUT` e validação `MAX_AUDIO_BYTES`.
- [ ] Cliente HTTP para `WHISPER_UPSTREAM_URL` com `X-API-Key` e `UPSTREAM_TIMEOUT`.
- [ ] **Liberar o áudio da memória** ao concluir cada job (sucesso ou falha).
- [ ] Backpressure: 429/503 + `Retry-After` quando a fila estiver cheia.
- [ ] Repasse fiel do corpo (`text`, `language`, `elapsed_ms`) e propagação de erros.
- [ ] Reivindicação de jobs órfãos (se Stream + consumer group).
- [ ] Dockerfile + compose (worker + Redis); deploy (definir host/porta público).
- [ ] Apontar clientes/dashboard para a URL do worker Go.

---

## 3. Motor de transcrição — Whisper

### 3.1. `https://whisper.lai.ia.br` (destino do worker Go)

Instância **hospedada** do `worker-whisper`. Mesmo contrato `POST /transcribe` +
`X-API-Key`. É o **destino final** do worker Go. Requer **API key** (header
`X-API-Key`), guardada como segredo do worker Go (`WHISPER_API_KEY`).

### 3.2. `worker-whisper/` (a base do serviço hospedado)

Serviço **FastAPI + OpenAI Whisper** com aceleração GPU. Arquivo: `worker-whisper/main.py`.

**Endpoints**

| Método/Rota          | Auth                         | Descrição                       |
|----------------------|------------------------------|---------------------------------|
| `GET /health`        | livre                        | `{ status, model, device }`     |
| `POST /transcribe`   | `X-API-Key` (se configurado) | recebe áudio, devolve transcrição |
| `GET /docs`, `/openapi.json` | livre                | Swagger UI / schema OpenAPI     |

**`POST /transcribe`** — `multipart/form-data`: `audio` (File; WAV/MP3/OGG/FLAC — o que
`soundfile`/`ffmpeg` decodificarem) e `language` (Form, default `pt`, ISO 639-1).

**Resposta (200):** `{ "text": "Olá, tudo bem?", "language": "pt", "elapsed_ms": 320 }`
**Erros:** 401 (X-API-Key inválido), 422 (áudio não decodificável), 500 (erro na transcrição).

**Pré-processamento** (`main.py:152`): `soundfile` → mono (média dos canais) →
**reamostra para 16 kHz** (`scipy.signal.resample_poly`); `fp16` em CUDA.

**Configuração (env):**

| Var              | Default | Descrição                                            |
|------------------|---------|------------------------------------------------------|
| `WHISPER_MODEL`  | `large` | `tiny`/`base`/`small`/`medium`/`large`               |
| `WHISPER_DEVICE` | `cuda`  | `cuda` ou `cpu`                                       |
| `API_KEY`        | (vazio) | se vazio, **sem autenticação**; obrigatório em prod  |
| `PORT`           | `8765`  | porta do uvicorn                                      |

**Deploy** (`docker-compose.yml` + `Dockerfile`): container `lai-worker-whisper`, base
`pytorch/pytorch:2.7.0-cuda12.8-cudnn9-runtime`, com `ffmpeg` + `libsndfile1`, reserva
1 GPU NVIDIA, expõe `8765`. Modelo carregado **uma vez no startup**, cacheado no volume
`whisper-cache`. Deps: `fastapi`, `uvicorn`, `python-multipart`, `openai-whisper`,
`torch`, `numpy`, `soundfile`, `scipy`.

---

## 4. Configuração e segredos

### Cadeia de API keys
```
Cliente  --X-API-Key (API_KEY do worker Go)-->  worker-go  --X-API-Key (WHISPER_API_KEY)-->  whisper.lai.ia.br
```

### Worker Go
Ver tabela de env em §2.5. Segredos (`API_KEY`, `WHISPER_API_KEY`) em secret manager,
nunca no código. Requer um **Redis** acessível (`REDIS_URL`).

### Whisper hospedado
`WHISPER_MODEL`, `WHISPER_DEVICE`, `API_KEY`, `PORT` (ver §3.2). **`API_KEY` obrigatória
em produção** — se vazia, o endpoint fica aberto.

---

## 5. Mapa de arquivos

| Arquivo | Papel |
|---------|-------|
| `worker-transcription/` *(a criar)* | **Worker Go** — entrada HTTP, fila Redis, consumo assíncrono e repasse para `whisper.lai.ia.br`. **Alvo da implementação.** |
| `worker-whisper/main.py` | **Motor de transcrição** — FastAPI + Whisper; base do serviço hospedado em `whisper.lai.ia.br` |
| `worker-whisper/{Dockerfile,docker-compose.yml,.env.example}` | Deploy do worker-whisper (GPU NVIDIA, porta 8765) |
| `worker/` (Go) | Worker de **treinamento** — **não** faz transcrição |
