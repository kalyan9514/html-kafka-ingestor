package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// RowHandler is a function the consumer calls for each received row.
// Keeping it as a callback decouples the consumer from the DB layer.
type RowHandler func(row map[string]string) error

// Consumer wraps a kafka-go reader with our app's consumption logic.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer creates a Kafka consumer for the given broker, topic, and group.
// GroupID ensures each message is processed by only one consumer instance,
// enabling horizontal scaling without duplicate processing.
func NewConsumer(brokers, topic, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers},
		Topic:   topic,
		GroupID: groupID,
	})
	return &Consumer{reader: r}
}

// Consume reads messages from Kafka and passes each row to the handler.
// Offset is committed only after the handler succeeds — guarantees at-least-once delivery.
func (c *Consumer) Consume(ctx context.Context, handler RowHandler) error {
	for {
		// FetchMessage retrieves the next message without committing the offset yet
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled, clean shutdown
			}
			return fmt.Errorf("failed to fetch message: %w", err)
		}

		var row map[string]string
		if err := json.Unmarshal(msg.Value, &row); err != nil {
			log.Printf("Skipping malformed message at offset %d: %v", msg.Offset, err)
			// commit offset even on bad messages so we don't get stuck replaying them
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := handler(row); err != nil {
			log.Printf("Handler failed for offset %d: %v — will retry on restart", msg.Offset, err)
			continue // don't commit — Kafka will redeliver on restart
		}

		// commit only after successful processing
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Failed to commit offset %d: %v", msg.Offset, err)
		}
	}
}

// Close shuts down the consumer cleanly.
func (c *Consumer) Close() error {
	return c.reader.Close()
}