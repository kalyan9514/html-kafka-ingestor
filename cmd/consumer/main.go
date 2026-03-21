package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kalyan9514/html-kafka-ingestor/config"
	"github.com/kalyan9514/html-kafka-ingestor/internal/db"
	kafkapkg "github.com/kalyan9514/html-kafka-ingestor/internal/kafka"
	"github.com/kalyan9514/html-kafka-ingestor/internal/metrics"
	"github.com/kalyan9514/html-kafka-ingestor/internal/parser"
	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load()

	m := metrics.New()
	metrics.StartServer(cfg.MetricsPort)

	database, err := db.New(cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer database.Close()

	// read directly from kafka-go so we can inspect message keys before unmarshalling
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBrokers},
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	})
	defer reader.Close()

	dlq := kafkapkg.NewDLQ(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer dlq.Close()

	var columns []parser.Column
	var buffer []map[string]string
	batchSize := 10
	tableCreated := false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("Shutdown signal received")
		cancel()
	}()

	log.Println("Consumer started, waiting for messages...")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Failed to fetch message: %v", err)
			continue
		}

		// schema message is identified by its key
		if string(msg.Key) == "schema" {
			if err := json.Unmarshal(msg.Value, &columns); err != nil {
				log.Printf("Failed to parse schema: %v", err)
				reader.CommitMessages(ctx, msg)
				continue
			}
			if err := database.CreateTable("ingested_data", columns); err != nil {
				log.Fatalf("Failed to create table: %v", err)
			}
			tableCreated = true
			log.Printf("Table created with %d columns", len(columns))
			reader.CommitMessages(ctx, msg)
			continue
		}

		if !tableCreated {
			log.Printf("Skipping row — table not ready yet")
			reader.CommitMessages(ctx, msg)
			continue
		}

		var row map[string]string
		if err := json.Unmarshal(msg.Value, &row); err != nil {
			log.Printf("Skipping malformed message at offset %d: %v", msg.Offset, err)
			reader.CommitMessages(ctx, msg)
			continue
		}

		buffer = append(buffer, row)

		if len(buffer) >= batchSize {
			start := time.Now()
			if err := database.BatchInsert("ingested_data", columns, buffer); err != nil {
				m.RowsFailed.Add(float64(len(buffer)))
				log.Printf("Batch insert failed: %v — sending to DLQ", err)
				for _, r := range buffer {
					dlq.Publish(ctx, r, fmt.Errorf("batch insert failed: %w", err))
				}
			} else {
				m.InsertDuration.Observe(time.Since(start).Seconds())
				m.RowsIngested.Add(float64(len(buffer)))
				log.Printf("Inserted batch of %d rows", len(buffer))
			}
			buffer = buffer[:0]
		}

		reader.CommitMessages(ctx, msg)
	}
}