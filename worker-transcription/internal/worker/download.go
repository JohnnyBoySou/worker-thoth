package worker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"
)

// errTooLarge signals the audio exceeded MAX_AUDIO_BYTES.
type errTooLarge struct{ limit int64 }

func (e *errTooLarge) Error() string {
	return fmt.Sprintf("audio exceeds MAX_AUDIO_BYTES (%d)", e.limit)
}

// download fetches audio from a URL into memory, enforcing a timeout and a hard
// byte limit. It checks the advertised Content-Length first and also caps the
// actual read with io.LimitReader so a lying/chunked server cannot blow past the
// limit. The returned bytes live only in memory.
func download(ctx context.Context, url string, maxBytes int64, timeout time.Duration) (string, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", nil, fmt.Errorf("build download request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("download returned status %d", resp.StatusCode)
	}
	if resp.ContentLength > maxBytes {
		return "", nil, &errTooLarge{limit: maxBytes}
	}

	// Read at most maxBytes+1 so we can detect overflow when Content-Length lies.
	limited := io.LimitReader(resp.Body, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return "", nil, fmt.Errorf("read download body: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return "", nil, &errTooLarge{limit: maxBytes}
	}

	return filenameFromURL(url), data, nil
}

func filenameFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Path == "" {
		return "audio"
	}
	base := path.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		return "audio"
	}
	return base
}
