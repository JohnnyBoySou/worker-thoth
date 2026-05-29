// Package job defines the transcription job domain model: its lifecycle status,
// the source of the audio (uploaded bytes vs. remote URL) and the serializable
// state persisted in Redis.
package job

// Status is the lifecycle state of a transcription job.
type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

// Source tells the worker where to obtain the audio for a job.
type Source string

const (
	// SourceUpload means the audio bytes live in the in-process audio store,
	// keyed by the job ID. Never persisted to disk or Redis.
	SourceUpload Source = "upload"
	// SourceURL means the worker must download the audio from Job.URL.
	SourceURL Source = "url"
)

// Job is the persisted state of a transcription request.
//
// The audio itself is intentionally NOT part of this struct: uploaded audio is
// held only in the in-memory audio store and URL audio is downloaded on demand.
// Only the resulting text (Result) is ever stored, and the whole record carries
// a TTL in Redis.
type Job struct {
	ID          string `json:"jobId"`
	Status      Status `json:"status"`
	Source      Source `json:"-"`
	URL         string `json:"-"`
	Language    string `json:"language,omitempty"`
	Result      string `json:"result,omitempty"`
	Error       string `json:"error,omitempty"`
	CreatedAt   string `json:"createdAt,omitempty"`
	CompletedAt string `json:"completedAt,omitempty"`
}
