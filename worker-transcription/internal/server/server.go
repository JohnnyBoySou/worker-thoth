// Package server wires the HTTP API: health, the two enqueue endpoints
// (multipart upload and URL) and job status polling. Handlers only enqueue work
// and read state — the actual transcription happens in the worker pool.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime"
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

// Handler returns the configured HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("POST /transcribe", s.auth(http.HandlerFunc(s.handleUpload)))
	mux.Handle("POST /transcribe/url", s.auth(http.HandlerFunc(s.handleURL)))
	mux.Handle("GET /jobs/{jobId}", s.auth(http.HandlerFunc(s.handleGetJob)))
	return mux
}

// auth enforces the X-API-Key header against the configured client API key,
// using a constant-time comparison.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if subtle.ConstantTimeCompare([]byte(key), []byte(s.cfg.APIKey)) != 1 {
			writeError(w, http.StatusUnauthorized, "invalid or missing X-API-Key")
			return
		}
		next.ServeHTTP(w, r)
	})
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
