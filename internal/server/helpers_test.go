package server

import (
	"encoding/json"
	"testing"

	"github.com/lai/worker-transcription/internal/job"
)

func TestIsHTTPURL(t *testing.T) {
	cases := map[string]bool{
		"https://whisper.lai.ia.br/a.mp3": true,
		"http://example.com/x.wav":        true,
		"ftp://example.com/x.wav":         false,
		"file:///etc/passwd":              false,
		"not a url":                       false,
		"":                                false,
		"https://":                        false,
	}
	for in, want := range cases {
		if got := isHTTPURL(in); got != want {
			t.Errorf("isHTTPURL(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("", "", "pt"); got != "pt" {
		t.Errorf("got %q, want pt", got)
	}
	if got := firstNonEmpty("en", "pt"); got != "en" {
		t.Errorf("got %q, want en", got)
	}
	if got := firstNonEmpty("", ""); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestJobViewForwardsWhisperBodyAsRawJSON(t *testing.T) {
	// Arrange
	j := job.Job{
		ID:          "abc",
		Status:      job.StatusCompleted,
		Result:      `{"text":"olá","language":"pt","elapsed_ms":320}`,
		CreatedAt:   "2026-05-29T00:00:00Z",
		CompletedAt: "2026-05-29T00:00:01Z",
	}

	// Act
	view := jobView(j)
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Assert: the result is embedded as an object, not a re-escaped string.
	var decoded struct {
		Result struct {
			Text     string `json:"text"`
			Language string `json:"language"`
		} `json:"result"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Result.Text != "olá" || decoded.Result.Language != "pt" {
		t.Fatalf("whisper body not forwarded intact: %s", encoded)
	}
	if decoded.Status != "completed" {
		t.Fatalf("unexpected status: %s", decoded.Status)
	}
}

func TestJobViewPlainStringResult(t *testing.T) {
	j := job.Job{ID: "x", Status: job.StatusCompleted, Result: "not-json"}
	if got := rawOrString(j.Result); got != "not-json" {
		t.Fatalf("expected plain string passthrough, got %v", got)
	}
}

func TestJobViewOmitsEmptyFields(t *testing.T) {
	j := job.Job{ID: "x", Status: job.StatusQueued, CreatedAt: "t"}
	view := jobView(j)
	if _, ok := view["result"]; ok {
		t.Error("result should be omitted when empty")
	}
	if _, ok := view["error"]; ok {
		t.Error("error should be omitted when empty")
	}
	if _, ok := view["completedAt"]; ok {
		t.Error("completedAt should be omitted when empty")
	}
}
