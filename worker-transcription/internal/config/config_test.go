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
