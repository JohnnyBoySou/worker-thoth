// Package redisstore implements the job queue and job-state store on top of
// Redis. The queue is a Redis Stream consumed by a consumer group (at-least-once
// delivery with XACK and XAUTOCLAIM for orphan recovery). Job state lives in a
// per-job hash (job:{id}) carrying a TTL so results expire on their own.
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lai/worker-transcription/internal/job"
	"github.com/redis/go-redis/v9"
)

const (
	streamKey  = "transcription:queue"
	groupName  = "workers"
	jobKeyFmt  = "job:%s"
	fieldJobID = "jobId"
)

// ErrQueueFull is returned by Enqueue when the queue depth has reached its cap.
var ErrQueueFull = errors.New("queue is full")

// ErrNotFound is returned when a job key does not exist (missing or TTL-expired).
var ErrNotFound = errors.New("job not found")

// Store wraps a Redis client and the queue/state operations.
type Store struct {
	rdb       *redis.Client
	jobTTL    time.Duration
	queueSize int
}

// New connects to Redis, applies the TTL/queue cap and ensures the consumer
// group exists.
func New(ctx context.Context, redisURL string, jobTTL time.Duration, queueSize int) (*Store, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	s := &Store{rdb: rdb, jobTTL: jobTTL, queueSize: queueSize}
	if err := s.ensureGroup(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Close releases the Redis connection.
func (s *Store) Close() error { return s.rdb.Close() }

// Ping checks Redis connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

func (s *Store) ensureGroup(ctx context.Context) error {
	// MKSTREAM creates the stream if absent; BUSYGROUP means it already exists.
	err := s.rdb.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

func jobKey(id string) string { return fmt.Sprintf(jobKeyFmt, id) }

// CreateAndEnqueue persists the initial job state and pushes the job ID onto the
// stream, atomically rejecting the request with ErrQueueFull when the queue is
// at capacity. The depth check counts entries still present in the stream
// (queued + in-flight), since acked entries are XDEL'd after processing.
func (s *Store) CreateAndEnqueue(ctx context.Context, j job.Job) error {
	length, err := s.rdb.XLen(ctx, streamKey).Result()
	if err != nil {
		return fmt.Errorf("queue length: %w", err)
	}
	if length >= int64(s.queueSize) {
		return ErrQueueFull
	}

	key := jobKey(j.ID)
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key, map[string]any{
		"status":    string(j.Status),
		"source":    string(j.Source),
		"url":       j.URL,
		"language":  j.Language,
		"createdAt": j.CreatedAt,
	})
	pipe.Expire(ctx, key, s.jobTTL)
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{fieldJobID: j.ID},
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}
	return nil
}

// Get loads a job's state. Returns ErrNotFound when the key is absent/expired.
func (s *Store) Get(ctx context.Context, id string) (job.Job, error) {
	vals, err := s.rdb.HGetAll(ctx, jobKey(id)).Result()
	if err != nil {
		return job.Job{}, fmt.Errorf("get job: %w", err)
	}
	if len(vals) == 0 {
		return job.Job{}, ErrNotFound
	}
	return job.Job{
		ID:          id,
		Status:      job.Status(vals["status"]),
		Source:      job.Source(vals["source"]),
		URL:         vals["url"],
		Language:    vals["language"],
		Result:      vals["result"],
		Error:       vals["error"],
		CreatedAt:   vals["createdAt"],
		CompletedAt: vals["completedAt"],
	}, nil
}

// SetProcessing marks a job as processing, refreshing the TTL.
func (s *Store) SetProcessing(ctx context.Context, id string) error {
	return s.update(ctx, id, map[string]any{"status": string(job.StatusProcessing)})
}

// SetCompleted stores the upstream result body and marks the job completed.
func (s *Store) SetCompleted(ctx context.Context, id, result, completedAt string) error {
	return s.update(ctx, id, map[string]any{
		"status":      string(job.StatusCompleted),
		"result":      result,
		"completedAt": completedAt,
	})
}

// SetFailed records the error and marks the job failed.
func (s *Store) SetFailed(ctx context.Context, id, errMsg, completedAt string) error {
	return s.update(ctx, id, map[string]any{
		"status":      string(job.StatusFailed),
		"error":       errMsg,
		"completedAt": completedAt,
	})
}

func (s *Store) update(ctx context.Context, id string, fields map[string]any) error {
	key := jobKey(id)
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, s.jobTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("update job %s: %w", id, err)
	}
	return nil
}

// QueuedMessage is a single unit of work pulled from the stream.
type QueuedMessage struct {
	MessageID string // Redis stream entry ID (for XACK/XDEL)
	JobID     string
}

// Read blocks up to the given timeout for new messages for this consumer.
func (s *Store) Read(ctx context.Context, consumer string, count int64, block time.Duration) ([]QueuedMessage, error) {
	res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumer,
		Streams:  []string{streamKey, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // timed out with no messages
	}
	if err != nil {
		return nil, fmt.Errorf("read group: %w", err)
	}
	return toMessages(res), nil
}

// Claim reclaims messages idle longer than minIdle from dead/stuck consumers,
// returning them to this consumer (orphan recovery). The cursor "0-0" scans from
// the start of the pending list.
func (s *Store) Claim(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]QueuedMessage, error) {
	msgs, _, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    groupName,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    count,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("autoclaim: %w", err)
	}
	return toXMessages(msgs), nil
}

// Ack acknowledges a message and deletes it from the stream so XLEN reflects the
// real backlog (queued + in-flight) for backpressure accounting.
func (s *Store) Ack(ctx context.Context, messageID string) error {
	pipe := s.rdb.TxPipeline()
	pipe.XAck(ctx, streamKey, groupName, messageID)
	pipe.XDel(ctx, streamKey, messageID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("ack message %s: %w", messageID, err)
	}
	return nil
}

// QueueLen reports the current stream depth (queued + in-flight).
func (s *Store) QueueLen(ctx context.Context) (int64, error) {
	return s.rdb.XLen(ctx, streamKey).Result()
}

func toMessages(streams []redis.XStream) []QueuedMessage {
	var out []QueuedMessage
	for _, st := range streams {
		out = append(out, toXMessages(st.Messages)...)
	}
	return out
}

func toXMessages(msgs []redis.XMessage) []QueuedMessage {
	out := make([]QueuedMessage, 0, len(msgs))
	for _, m := range msgs {
		id, _ := m.Values[fieldJobID].(string)
		out = append(out, QueuedMessage{MessageID: m.ID, JobID: id})
	}
	return out
}
