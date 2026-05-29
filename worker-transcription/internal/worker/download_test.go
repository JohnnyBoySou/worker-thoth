package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDownloadSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("audio-bytes"))
	}))
	defer srv.Close()

	name, data, err := download(context.Background(), srv.URL+"/clip.mp3", 1024, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "audio-bytes" {
		t.Errorf("data = %q", data)
	}
	if name != "clip.mp3" {
		t.Errorf("filename = %q, want clip.mp3", name)
	}
}

func TestDownloadRejectsViaContentLength(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 100)))
	}))
	defer srv.Close()

	_, _, err := download(context.Background(), srv.URL, 10, 5*time.Second)
	var tooLarge *errTooLarge
	if !errors.As(err, &tooLarge) {
		t.Fatalf("expected errTooLarge, got %v", err)
	}
}

func TestDownloadRejectsViaStreamWhenContentLengthHidden(t *testing.T) {
	// Chunked response hides Content-Length; the io.LimitReader guard must catch it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fl, _ := w.(http.Flusher)
		for i := 0; i < 50; i++ {
			_, _ = w.Write([]byte("xxxxx"))
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer srv.Close()

	_, _, err := download(context.Background(), srv.URL, 10, 5*time.Second)
	var tooLarge *errTooLarge
	if !errors.As(err, &tooLarge) {
		t.Fatalf("expected errTooLarge from stream guard, got %v", err)
	}
}

func TestDownloadNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, _, err := download(context.Background(), srv.URL, 1024, 5*time.Second)
	if err == nil {
		t.Fatal("expected error for non-200")
	}
}

func TestFilenameFromURL(t *testing.T) {
	cases := map[string]string{
		"https://x.com/a/b/song.wav":      "song.wav",
		"https://x.com/song.mp3?token=ab": "song.mp3",
		"https://x.com/":                  "audio",
		"https://x.com":                   "audio",
	}
	for in, want := range cases {
		if got := filenameFromURL(in); got != want {
			t.Errorf("filenameFromURL(%q) = %q, want %q", in, got, want)
		}
	}
}
