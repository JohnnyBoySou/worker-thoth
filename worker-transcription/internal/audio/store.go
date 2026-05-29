// Package audio provides an in-process, ephemeral store for uploaded audio.
//
// Uploaded audio bytes never touch disk or Redis. They live here only between
// the moment a POST /transcribe request is accepted and the moment the worker
// finishes (success or failure), after which Take/Drop removes the entry and the
// buffer is zeroed so the GC can reclaim it promptly.
//
// Because the store is per-process, uploaded jobs must be processed by the same
// instance that accepted them. URL jobs do not use this store.
package audio

import "sync"

// Clip is a single in-memory audio buffer.
type Clip struct {
	Filename string
	Data     []byte
}

// Store is a concurrency-safe map of jobID -> audio clip.
type Store struct {
	mu sync.Mutex
	m  map[string]*Clip
}

// NewStore creates an empty audio store.
func NewStore() *Store {
	return &Store{m: make(map[string]*Clip)}
}

// Put stores the clip for a job ID, replacing any existing entry.
func (s *Store) Put(jobID string, clip *Clip) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[jobID] = clip
}

// Take removes and returns the clip for a job ID. The boolean reports whether a
// clip was present. The caller owns the returned buffer and should Zero it once
// the audio is no longer needed.
func (s *Store) Take(jobID string) (*Clip, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	clip, ok := s.m[jobID]
	delete(s.m, jobID)
	return clip, ok
}

// Drop removes the clip for a job ID without returning it, zeroing the buffer.
// Safe to call when no clip exists (e.g. URL jobs or already-taken uploads).
func (s *Store) Drop(jobID string) {
	s.mu.Lock()
	clip := s.m[jobID]
	delete(s.m, jobID)
	s.mu.Unlock()
	if clip != nil {
		Zero(clip)
	}
}

// Len returns the number of audio clips currently held in memory.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

// Zero clears the buffer contents and drops the slice reference so the audio is
// released from memory as soon as possible.
func Zero(clip *Clip) {
	if clip == nil {
		return
	}
	for i := range clip.Data {
		clip.Data[i] = 0
	}
	clip.Data = nil
}
