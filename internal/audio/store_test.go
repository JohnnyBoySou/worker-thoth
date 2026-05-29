package audio

import "testing"

func TestPutTakeRemovesEntry(t *testing.T) {
	// Arrange
	s := NewStore()
	s.Put("job1", &Clip{Filename: "a.wav", Data: []byte("hello")})

	// Act
	clip, ok := s.Take("job1")

	// Assert
	if !ok {
		t.Fatal("expected clip to be present")
	}
	if clip.Filename != "a.wav" || string(clip.Data) != "hello" {
		t.Fatalf("unexpected clip: %+v", clip)
	}
	if s.Len() != 0 {
		t.Fatalf("expected store empty after Take, got %d", s.Len())
	}
	if _, ok := s.Take("job1"); ok {
		t.Fatal("expected second Take to miss")
	}
}

func TestDropZeroesBufferAndRemoves(t *testing.T) {
	// Arrange
	s := NewStore()
	data := []byte("secret-audio")
	clip := &Clip{Filename: "x", Data: data}
	s.Put("job2", clip)

	// Act
	s.Drop("job2")

	// Assert
	if s.Len() != 0 {
		t.Fatalf("expected empty store, got %d", s.Len())
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("expected buffer zeroed at %d, got %d", i, b)
		}
	}
}

func TestDropMissingIsNoop(t *testing.T) {
	s := NewStore()
	s.Drop("nope") // must not panic
	if s.Len() != 0 {
		t.Fatal("expected empty store")
	}
}
