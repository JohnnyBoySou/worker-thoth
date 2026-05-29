package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/lai/worker-transcription/internal/job"
)

// errEOF is io.EOF, aliased so server.go avoids importing io directly.
var errEOF = io.EOF

// nowRFC3339 returns the current UTC time for job timestamps.
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// newQueuedJob builds the initial job record in the queued state.
func newQueuedJob(id string, source job.Source, jobURL, language string) job.Job {
	return job.Job{
		ID:        id,
		Status:    job.StatusQueued,
		Source:    source,
		URL:       jobURL,
		Language:  language,
		CreatedAt: nowRFC3339(),
	}
}

// jobView is the public JSON shape returned by GET /jobs/{id}. The Whisper body
// is forwarded verbatim as raw JSON when present.
func jobView(j job.Job) map[string]any {
	view := map[string]any{
		"jobId":     j.ID,
		"status":    string(j.Status),
		"createdAt": j.CreatedAt,
	}
	if j.CompletedAt != "" {
		view["completedAt"] = j.CompletedAt
	}
	if j.Error != "" {
		view["error"] = j.Error
	}
	if j.Result != "" {
		view["result"] = rawOrString(j.Result)
	}
	return view
}

// rawOrString returns the result as parsed JSON when it is valid JSON (so the
// Whisper body is passed through intact), otherwise as a plain string.
func rawOrString(s string) any {
	if json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	return s
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func errString(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}

func isHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
