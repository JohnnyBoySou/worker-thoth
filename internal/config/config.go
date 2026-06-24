// Package config loads and validates the worker configuration from environment
// variables. Secrets (API_KEY, WHISPER_API_KEY) are read from the environment
// and never hardcoded.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/lai/worker-transcription/internal/window"
)

// Config holds all runtime settings for the transcription worker.
type Config struct {
	Port               string
	APIKey             string
	RedisURL           string
	WhisperUpstreamURL string
	WhisperAPIKey      string
	MaxConcurrency     int
	QueueSize          int
	JobTTL             time.Duration
	MaxAudioBytes      int64
	DownloadTimeout    time.Duration
	UpstreamTimeout    time.Duration
	DefaultLanguage    string
	// WhisperWindow restricts when the worker processes jobs (sends to Whisper).
	// Disabled (always open) unless WHISPER_WINDOW_START/END are set.
	WhisperWindow window.Window
}

// Load reads configuration from the environment, applies defaults and validates
// required values. It fails fast (returns an error) when a required secret or
// connection string is missing.
func Load() (Config, error) {
	cfg := Config{
		Port:               getEnv("PORT", "8770"),
		APIKey:             os.Getenv("API_KEY"),
		RedisURL:           getEnv("REDIS_URL", "redis://localhost:6379/0"),
		WhisperUpstreamURL: getEnv("WHISPER_UPSTREAM_URL", "https://whisper.lai.ia.br"),
		WhisperAPIKey:      os.Getenv("WHISPER_API_KEY"),
		DefaultLanguage:    getEnv("DEFAULT_LANGUAGE", "pt"),
	}

	var err error
	if cfg.MaxConcurrency, err = getEnvInt("MAX_CONCURRENCY", 2); err != nil {
		return Config{}, err
	}
	if cfg.QueueSize, err = getEnvInt("QUEUE_SIZE", 5000); err != nil {
		return Config{}, err
	}
	if cfg.MaxAudioBytes, err = getEnvInt64("MAX_AUDIO_BYTES", 104857600); err != nil {
		return Config{}, err
	}
	if cfg.JobTTL, err = getEnvDuration("JOB_TTL", time.Hour); err != nil {
		return Config{}, err
	}
	if cfg.DownloadTimeout, err = getEnvDuration("DOWNLOAD_TIMEOUT", 600*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.UpstreamTimeout, err = getEnvDuration("UPSTREAM_TIMEOUT", 600*time.Second); err != nil {
		return Config{}, err
	}

	// Processing window (when the worker is allowed to send to Whisper). Empty
	// start/end → always open. Invalid values fail fast at startup.
	cfg.WhisperWindow, err = window.Parse(
		os.Getenv("WHISPER_WINDOW_START"),
		os.Getenv("WHISPER_WINDOW_END"),
		getEnv("WHISPER_WINDOW_TZ", "America/Sao_Paulo"),
	)
	if err != nil {
		return Config{}, fmt.Errorf("WHISPER_WINDOW config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.APIKey == "" {
		return fmt.Errorf("API_KEY is required (client authentication)")
	}
	if c.WhisperAPIKey == "" {
		return fmt.Errorf("WHISPER_API_KEY is required (upstream authentication)")
	}
	if c.RedisURL == "" {
		return fmt.Errorf("REDIS_URL is required")
	}
	if c.WhisperUpstreamURL == "" {
		return fmt.Errorf("WHISPER_UPSTREAM_URL is required")
	}
	if c.MaxConcurrency < 1 {
		return fmt.Errorf("MAX_CONCURRENCY must be >= 1, got %d", c.MaxConcurrency)
	}
	if c.QueueSize < 1 {
		return fmt.Errorf("QUEUE_SIZE must be >= 1, got %d", c.QueueSize)
	}
	if c.MaxAudioBytes < 1 {
		return fmt.Errorf("MAX_AUDIO_BYTES must be >= 1, got %d", c.MaxAudioBytes)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	return n, nil
}

func getEnvInt64(key string, fallback int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	return n, nil
}

// getEnvDuration accepts Go duration strings ("1h", "600s", "500ms").
func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	return d, nil
}
