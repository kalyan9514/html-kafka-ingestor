package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer wraps a kafka-go writer with our app's publishing logic.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a Kafka producer for the given broker and topic.
// BatchSize and BatchTimeout enable micro-batching — reduces 51 network round trips to 1.
func NewProducer(brokers, topic string) *Producer {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{brokers},
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	})
	return &Producer{writer: w}
}

// PublishRow serialises a single row as JSON and publishes it to Kafka.
// Each message is keyed by row index so consumers can detect duplicates.
func (p *Producer) PublishRow(ctx context.Context, rowIndex int, row map[string]string) error {
	data, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("failed to serialise row %d: %w", rowIndex, err)
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("row-%d", rowIndex)),
		Value: data,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish row %d: %w", rowIndex, err)
	}

	log.Printf("Published row %d to Kafka", rowIndex)
	return nil
}

// Close flushes any pending messages and closes the connection cleanly.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// PublishSchema sends the inferred column schema as the first message.
// Consumer uses this to create the MySQL table before processing rows.
func (p *Producer) PublishSchema(ctx context.Context, schemaJSON []byte) error {
	msg := kafka.Message{
		Key:   []byte("schema"),
		Value: schemaJSON,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish schema: %w", err)
	}
	log.Println("Published schema to Kafka")
	return nil
}