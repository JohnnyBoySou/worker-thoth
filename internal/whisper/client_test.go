package whisper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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

func TestTranscribeSerializesUpstreamCalls(t *testing.T) {
	// The server records the peak number of concurrent in-flight requests. With
	// the gate, two concurrent Transcribe calls must never overlap upstream.
	var current, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := current.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond) // hold the "GPU" so an overlap would show
		current.Add(-1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"text":"ok"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "k", 5*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Transcribe(context.Background(), "a", []byte("x"), "pt"); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := peak.Load(); got != 1 {
		t.Errorf("peak concurrent upstream calls = %d, want 1", got)
	}
}

func TestTranscribeGateRespectsContext(t *testing.T) {
	// A slow upstream holds the gate; a second call with an already-cancelled
	// context must return immediately instead of blocking on the gate.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	c := New(srv.URL, "k", 5*time.Second)

	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = c.Transcribe(context.Background(), "a", []byte("x"), "pt") // holds the gate
	}()
	<-started
	time.Sleep(20 * time.Millisecond) // let the first call grab the gate

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Transcribe(ctx, "b", []byte("y"), "pt")
	if err == nil {
		t.Fatal("expected error from cancelled context while gate is held")
	}
	if !IsTransient(err) {
		t.Errorf("gate-wait cancellation should be transient, got: %v", err)
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
