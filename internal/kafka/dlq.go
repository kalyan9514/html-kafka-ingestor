package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// FailedRecord wraps a row that could not be processed along with the reason.
// Stored in the DLQ topic so failures can be investigated and replayed later.
type FailedRecord struct {
	Row       map[string]string `json:"row"`
	Error     string            `json:"error"`
	FailedAt  time.Time         `json:"failed_at"`
}

// DLQ publishes unprocessable rows to a separate Kafka topic instead of dropping them.
// This prevents data loss while keeping the main pipeline unblocked.
type DLQ struct {
	writer *kafka.Writer
}

// NewDLQ creates a producer pointed at the dead letter topic.
// By convention we name it <main-topic>-failed to make it discoverable.
func NewDLQ(brokers, mainTopic string) *DLQ {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{brokers},
		Topic:        mainTopic + "-failed",
		Balancer:     &kafka.RoundRobin{},
		WriteTimeout: 10 * time.Second,
	})
	return &DLQ{writer: w}
}

// Publish sends a failed row to the DLQ with the error reason attached.
// Called by the consumer when a DB insert fails after exhausting retries.
func (d *DLQ) Publish(ctx context.Context, row map[string]string, reason error) error {
	record := FailedRecord{
		Row:      row,
		Error:    reason.Error(),
		FailedAt: time.Now(),
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to serialise DLQ record: %w", err)
	}

	if err := d.writer.WriteMessages(ctx, kafka.Message{Value: data}); err != nil {
		return fmt.Errorf("failed to publish to DLQ: %w", err)
	}

	log.Printf("Row sent to DLQ: %v", reason)
	return nil
}

// Close shuts down the DLQ writer cleanly.
func (d *DLQ) Close() error {
	return d.writer.Close()
}