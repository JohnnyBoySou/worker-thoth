// Package worker runs the pool of goroutines that consume the Redis queue,
// fetch the audio (uploaded bytes or a URL download), forward it to Whisper,
// persist the result and then release the audio from memory.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lai/worker-transcription/internal/audio"
	"github.com/lai/worker-transcription/internal/job"
	"github.com/lai/worker-transcription/internal/redisstore"
	"github.com/lai/worker-transcription/internal/whisper"
)

const (
	readBlock      = 5 * time.Second
	claimInterval  = 30 * time.Second
	claimMinIdle   = 2 * time.Minute
	maxRetries     = 2
	retryBaseDelay = time.Second
)

// Pool consumes the queue with a fixed number of worker goroutines.
type Pool struct {
	store           *redisstore.Store
	audio           *audio.Store
	whisper         *whisper.Client
	logger          *slog.Logger
	concurrency     int
	maxAudioBytes   int64
	downloadTimeout time.Duration
	defaultLanguage string

	inFlight atomic.Int64
}

// Config carries the dependencies and tunables for the pool.
type Config struct {
	Store           *redisstore.Store
	Audio           *audio.Store
	Whisper         *whisper.Client
	Logger          *slog.Logger
	Concurrency     int
	MaxAudioBytes   int64
	DownloadTimeout time.Duration
	DefaultLanguage string
}

// New creates a worker pool.
func New(cfg Config) *Pool {
	return &Pool{
		store:           cfg.Store,
		audio:           cfg.Audio,
		whisper:         cfg.Whisper,
		logger:          cfg.Logger,
		concurrency:     cfg.Concurrency,
		maxAudioBytes:   cfg.MaxAudioBytes,
		downloadTimeout: cfg.DownloadTimeout,
		defaultLanguage: cfg.DefaultLanguage,
	}
}

// InFlight reports how many jobs are currently being processed.
func (p *Pool) InFlight() int64 { return p.inFlight.Load() }

// Run starts the worker goroutines plus the orphan-claim loop and blocks until
// the context is cancelled.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.concurrency; i++ {
		consumer := fmt.Sprintf("worker-%d", i)
		wg.Go(func() { p.consume(ctx, consumer) })
	}
	wg.Go(func() { p.claimLoop(ctx, "claimer") })
	wg.Wait()
}

// consume is the main loop for a single worker goroutine.
func (p *Pool) consume(ctx context.Context, consumer string) {
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, err := p.store.Read(ctx, consumer, 1, readBlock)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("queue read failed", "consumer", consumer, "err", err)
			sleep(ctx, retryBaseDelay)
			continue
		}
		for _, m := range msgs {
			p.handle(ctx, consumer, m)
		}
	}
}

// claimLoop periodically reclaims jobs abandoned by dead/stuck workers.
func (p *Pool) claimLoop(ctx context.Context, consumer string) {
	ticker := time.NewTicker(claimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msgs, err := p.store.Claim(ctx, consumer, claimMinIdle, 10)
			if err != nil {
				p.logger.Error("orphan claim failed", "err", err)
				continue
			}
			for _, m := range msgs {
				p.logger.Warn("reclaimed orphan job", "jobId", m.JobID, "messageId", m.MessageID)
				p.handle(ctx, consumer, m)
			}
		}
	}
}

// handle processes a single message end-to-end: fetch audio, transcribe, persist
// and release the audio. The stream message is always acked at the end so a
// permanently failing job does not loop forever.
func (p *Pool) handle(ctx context.Context, consumer string, m redisstore.QueuedMessage) {
	p.inFlight.Add(1)
	defer p.inFlight.Add(-1)
	// Always free any uploaded audio for this job, whatever the outcome.
	defer p.audio.Drop(m.JobID)

	start := time.Now()
	log := p.logger.With("jobId", m.JobID, "consumer", consumer)

	j, err := p.store.Get(ctx, m.JobID)
	if err != nil {
		// Job state gone (TTL-expired or never written): drop the message.
		log.Warn("job state missing; acking", "err", err)
		p.ack(ctx, m, log)
		return
	}
	if j.Status == job.StatusCompleted || j.Status == job.StatusFailed {
		p.ack(ctx, m, log) // already terminal (e.g. duplicate delivery)
		return
	}

	if err := p.store.SetProcessing(ctx, m.JobID); err != nil {
		log.Error("mark processing failed", "err", err)
	}

	language := j.Language
	if language == "" {
		language = p.defaultLanguage
	}

	filename, data, fetchErr := p.fetchAudio(ctx, m.JobID, j)
	if fetchErr != nil {
		p.fail(ctx, m, log, fmt.Errorf("fetch audio: %w", fetchErr))
		return
	}
	audioBytes := len(data)

	result, txErr := p.transcribeWithRetry(ctx, log, filename, data, language)
	// Release audio from memory immediately after the upstream call returns,
	// before we touch Redis again (success or failure).
	for i := range data {
		data[i] = 0
	}
	data = nil

	if txErr != nil {
		p.fail(ctx, m, log, txErr)
		return
	}

	completedAt := nowRFC3339()
	if err := p.store.SetCompleted(ctx, m.JobID, result.Body, completedAt); err != nil {
		log.Error("store result failed", "err", err)
	}
	p.ack(ctx, m, log)
	log.Info("job completed",
		"source", j.Source,
		"audioBytes", audioBytes,
		"durationMs", time.Since(start).Milliseconds(),
	)
}

// fetchAudio obtains the audio bytes for a job from the in-memory store (upload)
// or by downloading the URL.
func (p *Pool) fetchAudio(ctx context.Context, jobID string, j job.Job) (string, []byte, error) {
	switch j.Source {
	case job.SourceUpload:
		clip, ok := p.audio.Take(jobID)
		if !ok {
			// Audio not in this process: typically a reclaimed upload after a
			// restart. Cannot recover — uploads are not persisted by design.
			return "", nil, errors.New("uploaded audio no longer in memory (instance restarted?)")
		}
		return clip.Filename, clip.Data, nil
	case job.SourceURL:
		return download(ctx, j.URL, p.maxAudioBytes, p.downloadTimeout)
	default:
		return "", nil, fmt.Errorf("unknown job source %q", j.Source)
	}
}

// transcribeWithRetry calls Whisper, retrying only transient failures with a
// bounded exponential backoff.
func (p *Pool) transcribeWithRetry(ctx context.Context, log *slog.Logger, filename string, data []byte, language string) (whisper.Result, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := retryBaseDelay << (attempt - 1)
			log.Warn("retrying transcription", "attempt", attempt, "delay", delay.String(), "err", lastErr)
			if !sleep(ctx, delay) {
				return whisper.Result{}, fmt.Errorf("cancelled during retry backoff: %w", ctx.Err())
			}
		}
		result, err := p.whisper.Transcribe(ctx, filename, data, language)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !whisper.IsTransient(err) {
			return whisper.Result{}, err // permanent (e.g. 4xx): do not retry
		}
	}
	return whisper.Result{}, fmt.Errorf("transcription failed after %d retries: %w", maxRetries, lastErr)
}

func (p *Pool) fail(ctx context.Context, m redisstore.QueuedMessage, log *slog.Logger, cause error) {
	log.Error("job failed", "err", cause)
	if err := p.store.SetFailed(ctx, m.JobID, cause.Error(), nowRFC3339()); err != nil {
		log.Error("store failure state failed", "err", err)
	}
	p.ack(ctx, m, log)
}

func (p *Pool) ack(ctx context.Context, m redisstore.QueuedMessage, log *slog.Logger) {
	if err := p.store.Ack(ctx, m.MessageID); err != nil {
		log.Error("ack failed", "messageId", m.MessageID, "err", err)
	}
}

// nowRFC3339 returns the current time formatted for the job timestamps.
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// sleep waits for d or returns false if the context is cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
