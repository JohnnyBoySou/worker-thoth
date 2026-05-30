// Package server wires the HTTP API: health, the two enqueue endpoints
// (multipart upload and URL) and job status polling. Handlers only enqueue work
// and read state — the actual transcription happens in the worker pool.
package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/lai/worker-transcription/internal/audio"
	"github.com/lai/worker-transcription/internal/config"
	"github.com/lai/worker-transcription/internal/job"
	"github.com/lai/worker-transcription/internal/redisstore"
)

// inFlightReporter lets the health endpoint read live worker stats without a
// hard dependency on the worker package.
type inFlightReporter interface {
	InFlight() int64
}

// Server holds the HTTP dependencies.
type Server struct {
	cfg     config.Config
	store   *redisstore.Store
	audio   *audio.Store
	pool    inFlightReporter
	logger  *slog.Logger
	newID   func() string
}

// New builds the server. newID generates unique job IDs.
func New(cfg config.Config, store *redisstore.Store, audioStore *audio.Store, pool inFlightReporter, logger *slog.Logger, newID func() string) *Server {
	return &Server{
		cfg:    cfg,
		store:  store,
		audio:  audioStore,
		pool:   pool,
		logger: logger,
		newID:  newID,
	}
}

// gatewayPrefix mirrors the hosted gateway's namespace so existing clients that
// call https://gateway.lai.ia.br/v1/audio/transcriptions/... keep working
// against this worker without any change.
const gatewayPrefix = "/v1/audio/transcriptions"

// Handler returns the configured HTTP mux. Each transcription route is exposed
// both under its native path and under the gateway-compatible alias.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)

	upload := s.auth(http.HandlerFunc(s.handleUpload))
	url := s.auth(http.HandlerFunc(s.handleURL))
	getJob := s.auth(http.HandlerFunc(s.handleGetJob))

	// Native routes.
	mux.Handle("POST /transcribe", upload)
	mux.Handle("POST /transcribe/url", url)
	mux.Handle("GET /jobs/{jobId}", getJob)

	// Gateway-compatible aliases.
	mux.Handle("POST "+gatewayPrefix, upload)
	mux.Handle("POST "+gatewayPrefix+"/url", url)
	mux.Handle("GET "+gatewayPrefix+"/jobs/{jobId}", getJob)

	return s.accessLog(mux)
}

// statusRecorder captures the response status code for access logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// accessLog logs every incoming request (method, path, status, duration). It
// records whether an auth credential was present — never the credential itself —
// so misrouted/unauthenticated calls are diagnosable without leaking secrets.
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"durationMs", time.Since(start).Milliseconds(),
			"hasApiKey", r.Header.Get("X-API-Key") != "",
			"hasBearer", strings.HasPrefix(strings.ToLower(r.Header.Get("Authorization")), "bearer "),
			"remoteAddr", r.RemoteAddr,
			"userAgent", r.UserAgent(),
		)
	})
}

// auth enforces the client API key using a constant-time comparison. The key is
// accepted either via the X-API-Key header or as an Authorization: Bearer token
// (for compatibility with gateway-style clients).
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := extractAPIKey(r)
		if subtle.ConstantTimeCompare([]byte(key), []byte(s.cfg.APIKey)) != 1 {
			s.logAuthFailure(r, key)
			writeError(w, http.StatusUnauthorized, "invalid or missing API key (X-API-Key or Authorization: Bearer)")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// logAuthFailure emits a TEMPORARY diagnostic on 401s to compare the presented
// credential against the configured key WITHOUT leaking either secret. It logs
// only the length, first 3 / last 4 chars, and an 8-hex-char SHA-256 prefix of
// each, plus the raw Authorization scheme. Remove once the client is fixed.
func (s *Server) logAuthFailure(r *http.Request, presented string) {
	fingerprint := func(v string) (int, string, string, string) {
		if v == "" {
			return 0, "", "", ""
		}
		sum := sha256.Sum256([]byte(v))
		head := v
		if len(head) > 3 {
			head = head[:3]
		}
		tail := v
		if len(tail) > 4 {
			tail = tail[len(tail)-4:]
		}
		return len(v), head, tail, hex.EncodeToString(sum[:])[:8]
	}
	pLen, pHead, pTail, pHash := fingerprint(presented)
	_, _, _, wantHash := fingerprint(s.cfg.APIKey)

	authHeader := r.Header.Get("Authorization")
	scheme := ""
	if i := strings.IndexByte(authHeader, ' '); i > 0 {
		scheme = authHeader[:i]
	}

	s.logger.Warn("auth failure diagnostic",
		"path", r.URL.Path,
		"authScheme", scheme,
		"presentedLen", pLen,
		"presentedHead", pHead,
		"presentedTail", pTail,
		"presentedHash8", pHash,
		"expectedHash8", wantHash,
		"matches", pHash == wantHash,
	)
}

// extractAPIKey reads the client key from X-API-Key first, falling back to an
// Authorization: Bearer <token> header.
func extractAPIKey(r *http.Request) string {
	if key := r.Header.Get("X-API-Key"); key != "" {
		return key
	}
	if auth := r.Header.Get("Authorization"); auth != "" {
		const prefix = "Bearer "
		if len(auth) > len(prefix) && strings.EqualFold(auth[:len(prefix)], prefix) {
			return strings.TrimSpace(auth[len(prefix):])
		}
	}
	return ""
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	queueLen, queueErr := s.store.QueueLen(ctx)
	redisOK := s.store.Ping(ctx) == nil

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	status := "ok"
	code := http.StatusOK
	if !redisOK {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	writeJSON(w, code, map[string]any{
		"status":        status,
		"queueDepth":    queueLen,
		"queueError":    errString(queueErr),
		"inFlight":      s.pool.InFlight(),
		"audioInMemory": s.audio.Len(),
		"redisOk":       redisOK,
		"upstream":      s.cfg.WhisperUpstreamURL,
		"heapAllocMB":   mem.HeapAlloc / (1 << 20),
	})
}

// urlRequest is the JSON body for POST /transcribe/url.
type urlRequest struct {
	URL      string `json:"url"`
	Language string `json:"language"`
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	// Cap the in-memory parse to MAX_AUDIO_BYTES (+ small multipart overhead).
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxAudioBytes+(1<<20))
	if err := r.ParseMultipartForm(s.cfg.MaxAudioBytes + (1 << 20)); err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "audio exceeds MAX_AUDIO_BYTES or malformed form")
		return
	}
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()

	file, header, err := r.FormFile("audio")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing required 'audio' file field")
		return
	}
	defer file.Close()

	if header.Size > s.cfg.MaxAudioBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "audio exceeds MAX_AUDIO_BYTES")
		return
	}

	data := make([]byte, 0, header.Size)
	buf := make([]byte, 32<<10)
	for {
		n, readErr := file.Read(buf)
		if n > 0 {
			data = append(data, buf[:n]...)
			if int64(len(data)) > s.cfg.MaxAudioBytes {
				writeError(w, http.StatusRequestEntityTooLarge, "audio exceeds MAX_AUDIO_BYTES")
				return
			}
		}
		if readErr != nil {
			if errors.Is(readErr, errEOF) {
				break
			}
			writeError(w, http.StatusBadRequest, "failed to read audio")
			return
		}
	}

	language := firstNonEmpty(r.FormValue("language"), s.cfg.DefaultLanguage)
	id := s.newID()

	// Stash the audio in memory BEFORE enqueueing so a worker can never pick up
	// the job before the bytes are available.
	s.audio.Put(id, &audio.Clip{Filename: header.Filename, Data: data})

	j := newQueuedJob(id, job.SourceUpload, "", language)
	if err := s.store.CreateAndEnqueue(r.Context(), j); err != nil {
		s.audio.Drop(id) // roll back the stashed audio
		s.handleEnqueueError(w, err)
		return
	}

	s.logger.Info("job enqueued", "jobId", id, "source", "upload", "bytes", len(data))
	writeJSON(w, http.StatusAccepted, map[string]string{"jobId": id, "status": string(job.StatusQueued)})
}

func (s *Server) handleURL(w http.ResponseWriter, r *http.Request) {
	var req urlRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.URL == "" {
		writeError(w, http.StatusBadRequest, "missing required 'url' field")
		return
	}
	if !isHTTPURL(req.URL) {
		writeError(w, http.StatusBadRequest, "url must be an absolute http(s) URL")
		return
	}

	language := firstNonEmpty(req.Language, s.cfg.DefaultLanguage)
	id := s.newID()

	j := newQueuedJob(id, job.SourceURL, req.URL, language)
	if err := s.store.CreateAndEnqueue(r.Context(), j); err != nil {
		s.handleEnqueueError(w, err)
		return
	}

	s.logger.Info("job enqueued", "jobId", id, "source", "url")
	writeJSON(w, http.StatusAccepted, map[string]string{"jobId": id, "status": string(job.StatusQueued)})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("jobId")
	j, err := s.store.Get(r.Context(), id)
	if errors.Is(err, redisstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found or expired")
		return
	}
	if err != nil {
		s.logger.Error("get job failed", "jobId", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, jobView(j))
}

func (s *Server) handleEnqueueError(w http.ResponseWriter, err error) {
	if errors.Is(err, redisstore.ErrQueueFull) {
		w.Header().Set("Retry-After", "30")
		writeError(w, http.StatusTooManyRequests, "queue is full, retry later")
		return
	}
	s.logger.Error("enqueue failed", "err", err)
	writeError(w, http.StatusInternalServerError, "failed to enqueue job")
}
