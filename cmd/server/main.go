// Command server is the entry point for the transcription worker: it loads
// config, connects to Redis, starts the worker pool and serves the HTTP API,
// shutting everything down gracefully on SIGINT/SIGTERM.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lai/worker-transcription/internal/audio"
	"github.com/lai/worker-transcription/internal/config"
	"github.com/lai/worker-transcription/internal/redisstore"
	"github.com/lai/worker-transcription/internal/server"
	"github.com/lai/worker-transcription/internal/whisper"
	"github.com/lai/worker-transcription/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// Root context cancelled on SIGINT/SIGTERM to drive graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := redisstore.New(ctx, cfg.RedisURL, cfg.JobTTL, cfg.QueueSize)
	if err != nil {
		return err
	}
	defer store.Close()

	audioStore := audio.NewStore()
	whisperClient := whisper.New(cfg.WhisperUpstreamURL, cfg.WhisperAPIKey, cfg.UpstreamTimeout)

	pool := worker.New(worker.Config{
		Store:           store,
		Audio:           audioStore,
		Whisper:         whisperClient,
		Logger:          logger,
		Concurrency:     cfg.MaxConcurrency,
		MaxAudioBytes:   cfg.MaxAudioBytes,
		DownloadTimeout: cfg.DownloadTimeout,
		DefaultLanguage: cfg.DefaultLanguage,
	})

	srv := server.New(cfg, store, audioStore, pool, logger, newJobID)
	httpServer := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 15 * time.Second,
	}

	var wg sync.WaitGroup

	// Worker pool.
	wg.Go(func() {
		logger.Info("worker pool starting", "concurrency", cfg.MaxConcurrency)
		pool.Run(ctx)
		logger.Info("worker pool stopped")
	})

	// HTTP server.
	wg.Go(func() {
		logger.Info("http server listening", "port", cfg.Port, "upstream", cfg.WhisperUpstreamURL)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "err", err)
			stop() // trigger shutdown of the rest
		}
	})

	<-ctx.Done()
	logger.Info("shutdown signal received; draining")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "err", err)
	}

	wg.Wait()
	logger.Info("shutdown complete")
	return nil
}

// newJobID returns a random 128-bit hex identifier.
func newJobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is catastrophic; fall back to time-based entropy.
		return hex.EncodeToString([]byte(time.Now().UTC().Format("20060102150405.000000000")))
	}
	return hex.EncodeToString(b[:])
}
