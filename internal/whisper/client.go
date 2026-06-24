// Package whisper is the HTTP client that forwards audio to the hosted Whisper
// service (WHISPER_UPSTREAM_URL) authenticated with X-API-Key. It returns the
// upstream response body verbatim so the worker can store it intact.
package whisper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"
)

// maxErrorBody caps how much of an upstream error body we read into messages.
const maxErrorBody = 4 << 10

// Client posts audio to the hosted Whisper /transcribe endpoint.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	// gate serializes the upstream call so the single Whisper GPU never receives
	// more than one transcription at a time, regardless of how many workers run.
	// Buffered to 1 = at most one in-flight Transcribe; others queue here.
	gate chan struct{}
}

// New builds a Whisper client. The timeout bounds the whole transcription call.
func New(baseURL, apiKey string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: timeout},
		gate:    make(chan struct{}, 1),
	}
}

// Result distinguishes a successful body from a retryable failure.
type Result struct {
	Body string
}

// transientError marks failures worth retrying (timeouts, 5xx, transport errors).
type transientError struct{ err error }

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

// IsTransient reports whether an error from Transcribe is safe to retry.
func IsTransient(err error) bool {
	var t *transientError
	return errors.As(err, &t)
}

// Transcribe sends audio (filename + bytes) and language as multipart/form-data
// and returns the raw upstream body. 4xx responses are permanent errors; 5xx,
// timeouts and transport failures are wrapped as transient.
func (c *Client) Transcribe(ctx context.Context, filename string, audio []byte, language string) (Result, error) {
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)

	part, err := mw.CreateFormFile("audio", safeFilename(filename))
	if err != nil {
		return Result{}, fmt.Errorf("build multipart: %w", err)
	}
	if _, err := part.Write(audio); err != nil {
		return Result{}, fmt.Errorf("write audio part: %w", err)
	}
	if err := mw.WriteField("language", language); err != nil {
		return Result{}, fmt.Errorf("write language field: %w", err)
	}
	if err := mw.Close(); err != nil {
		return Result{}, fmt.Errorf("close multipart: %w", err)
	}

	url := c.baseURL + "/transcribe"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return Result{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-API-Key", c.apiKey)

	// Serialize the upstream call: only one transcription hits the GPU at a time.
	// Acquiring respects ctx so a cancelled/timed-out job never blocks the queue.
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return Result{}, &transientError{err: fmt.Errorf("whisper gate wait: %w", ctx.Err())}
	}

	resp, err := c.http.Do(req)
	if err != nil {
		// Transport errors (incl. context deadline) are treated as transient.
		return Result{}, &transientError{err: fmt.Errorf("whisper request: %w", err)}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, &transientError{err: fmt.Errorf("read whisper response: %w", err)}
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		return Result{Body: string(respBody)}, nil
	case resp.StatusCode >= 500:
		return Result{}, &transientError{err: upstreamErr(resp.StatusCode, respBody)}
	default: // 4xx and others: permanent
		return Result{}, upstreamErr(resp.StatusCode, respBody)
	}
}

func upstreamErr(status int, body []byte) error {
	if len(body) > maxErrorBody {
		body = body[:maxErrorBody]
	}
	return fmt.Errorf("whisper upstream returned %d: %s", status, string(body))
}

func safeFilename(name string) string {
	if name == "" {
		return "audio"
	}
	return name
}
