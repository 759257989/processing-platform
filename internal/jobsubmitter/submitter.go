// Package jobsubmitter implements the handlers.JobSubmitter interface so
// handlers can enqueue follow-up jobs (cross-tier and same-tier).
//
// Mirrors the API's submit path: INSERT a jobs row, publish to Kafka, then
// bump state to QUEUED. The previous "publish-only" implementation was a
// shortcut that left workers unable to GetJob the follow-up id.
package jobsubmitter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/759257989/processing-platform/internal/jobs"
	"github.com/759257989/processing-platform/internal/kafka"
	"github.com/759257989/processing-platform/internal/store"
	"github.com/759257989/processing-platform/internal/store/db"
)

type Submitter struct {
	Store    *store.Store
	Producer *kafka.Producer
}

func New(p *kafka.Producer, st *store.Store) *Submitter {
	return &Submitter{Store: st, Producer: p}
}

func (s *Submitter) Submit(ctx context.Context, typ jobs.Type, deviceID, idempKey string, payload []byte) error {
	tier, err := jobs.TierFor(typ)
	if err != nil {
		return fmt.Errorf("tier: %w", err)
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	job, err := s.Store.Queries.CreateJob(ctx, db.CreateJobParams{
		Type:           string(typ),
		Tier:           string(tier),
		DeviceID:       deviceID,
		Payload:        payload,
		IdempotencyKey: idempKey,
		MaxAttempts:    3,
	})
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}

	msg, _ := json.Marshal(map[string]any{
		"job_id":    job.ID.String(),
		"type":      job.Type,
		"tier":      job.Tier,
		"device_id": job.DeviceID,
		"payload":   json.RawMessage(job.Payload),
	})
	if err := s.Producer.Publish(ctx, "jobs."+string(tier), []byte(deviceID), msg); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	if _, err := s.Store.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
		ID:    job.ID,
		State: string(jobs.StateQueued),
	}); err != nil {
		return nil
	}
	return nil
}
