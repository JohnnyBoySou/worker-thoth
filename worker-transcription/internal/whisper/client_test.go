package whisper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTranscribeForwardsAudioAndKey(t *testing.T) {
	// Arrange
	var gotKey, gotLang, gotFilename string
	var gotAudio []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-API-Key")
		_ = r.ParseMultipartForm(1 << 20)
		gotLang = r.FormValue("language")
		f, h, _ := r.FormFile("audio")
		defer f.Close()
		gotFilename = h.Filename
		buf := make([]byte, h.Size)
		_, _ = f.Read(buf)
		gotAudio = buf
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"text":"olá","language":"pt","elapsed_ms":12}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "secret-key", 5*time.Second)

	// Act
	res, err := c.Transcribe(context.Background(), "clip.wav", []byte("RIFFDATA"), "pt")

	// Assert
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotKey != "secret-key" {
		t.Errorf("X-API-Key = %q, want secret-key", gotKey)
	}
	if gotLang != "pt" {
		t.Errorf("language = %q, want pt", gotLang)
	}
	if gotFilename != "clip.wav" {
		t.Errorf("filename = %q, want clip.wav", gotFilename)
	}
	if string(gotAudio) != "RIFFDATA" {
		t.Errorf("audio = %q, want RIFFDATA", gotAudio)
	}
	if res.Body != `{"text":"olá","language":"pt","elapsed_ms":12}` {
		t.Errorf("body not forwarded verbatim: %q", res.Body)
	}
}

func TestTranscribe4xxIsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte("cannot decode"))
	}))
	defer srv.Close()

	c := New(srv.URL, "k", 5*time.Second)
	_, err := c.Transcribe(context.Background(), "a", []byte("x"), "pt")
	if err == nil {
		t.Fatal("expected error")
	}
	if IsTransient(err) {
		t.Errorf("4xx must be permanent, got transient: %v", err)
	}
}

func TestTranscribe5xxIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	c := New(srv.URL, "k", 5*time.Second)
	_, err := c.Transcribe(context.Background(), "a", []byte("x"), "pt")
	if err == nil {
		t.Fatal("expected error")
	}
	if !IsTransient(err) {
		t.Errorf("5xx must be transient, got permanent: %v", err)
	}
}
