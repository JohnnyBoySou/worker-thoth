package config

import (
	"testing"
	"time"
)

func TestLoadAppliesDefaults(t *testing.T) {
	t.Setenv("API_KEY", "client-key")
	t.Setenv("WHISPER_API_KEY", "upstream-key")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != "8770" {
		t.Errorf("Port = %q, want 8770", cfg.Port)
	}
	if cfg.MaxConcurrency != 2 {
		t.Errorf("MaxConcurrency = %d, want 2", cfg.MaxConcurrency)
	}
	if cfg.JobTTL != time.Hour {
		t.Errorf("JobTTL = %v, want 1h", cfg.JobTTL)
	}
	if cfg.DefaultLanguage != "pt" {
		t.Errorf("DefaultLanguage = %q, want pt", cfg.DefaultLanguage)
	}
}

func TestLoadRequiresSecrets(t *testing.T) {
	t.Setenv("API_KEY", "")
	t.Setenv("WHISPER_API_KEY", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when API_KEY missing")
	}
}

func TestLoadParsesDurationsAndSizes(t *testing.T) {
	t.Setenv("API_KEY", "a")
	t.Setenv("WHISPER_API_KEY", "b")
	t.Setenv("JOB_TTL", "30m")
	t.Setenv("UPSTREAM_TIMEOUT", "120s")
	t.Setenv("MAX_AUDIO_BYTES", "2048")
	t.Setenv("MAX_CONCURRENCY", "4")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.JobTTL != 30*time.Minute {
		t.Errorf("JobTTL = %v, want 30m", cfg.JobTTL)
	}
	if cfg.UpstreamTimeout != 120*time.Second {
		t.Errorf("UpstreamTimeout = %v", cfg.UpstreamTimeout)
	}
	if cfg.MaxAudioBytes != 2048 {
		t.Errorf("MaxAudioBytes = %d", cfg.MaxAudioBytes)
	}
	if cfg.MaxConcurrency != 4 {
		t.Errorf("MaxConcurrency = %d", cfg.MaxConcurrency)
	}
}

func TestLoadRejectsBadDuration(t *testing.T) {
	t.Setenv("API_KEY", "a")
	t.Setenv("WHISPER_API_KEY", "b")
	t.Setenv("JOB_TTL", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for bad JOB_TTL")
	}
}

func TestLoadDefaultQueueSizeAndWindowDisabled(t *testing.T) {
	t.Setenv("API_KEY", "a")
	t.Setenv("WHISPER_API_KEY", "b")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.QueueSize != 5000 {
		t.Errorf("QueueSize = %d, want 5000", cfg.QueueSize)
	}
	if cfg.WhisperWindow.Enabled() {
		t.Error("window must be disabled when WHISPER_WINDOW_START/END are unset")
	}
}

func TestLoadParsesWindow(t *testing.T) {
	t.Setenv("API_KEY", "a")
	t.Setenv("WHISPER_API_KEY", "b")
	t.Setenv("WHISPER_WINDOW_START", "00:00")
	t.Setenv("WHISPER_WINDOW_END", "06:00")
	t.Setenv("WHISPER_WINDOW_TZ", "America/Sao_Paulo")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.WhisperWindow.Enabled() {
		t.Fatal("window should be enabled")
	}
}

func TestLoadRejectsBadWindow(t *testing.T) {
	t.Setenv("API_KEY", "a")
	t.Setenv("WHISPER_API_KEY", "b")
	t.Setenv("WHISPER_WINDOW_START", "00:00") // end missing → invalid
	if _, err := Load(); err == nil {
		t.Fatal("expected error for incomplete window config")
	}
}
