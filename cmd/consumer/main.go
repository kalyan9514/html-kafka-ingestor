package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kalyan9514/html-kafka-ingestor/config"
	"github.com/kalyan9514/html-kafka-ingestor/internal/db"
	"github.com/kalyan9514/html-kafka-ingestor/internal/kafka"
	"github.com/kalyan9514/html-kafka-ingestor/internal/metrics"
	"github.com/kalyan9514/html-kafka-ingestor/internal/parser"
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

	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	// buffer collects rows before batch inserting — reduces DB round trips
	var buffer []map[string]string
	var columns []parser.Column
	batchSize := 10

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// listen for Ctrl+C or kill signal for graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("Shutdown signal received")
		cancel()
	}()

	log.Println("Consumer started, waiting for messages...")

	err = consumer.Consume(ctx, func(row map[string]string) error {
		buffer = append(buffer, row)

		// flush buffer to DB once we have enough rows
		if len(buffer) >= batchSize {
			start := time.Now()
			if err := database.BatchInsert("ingested_data", columns, buffer); err != nil {
				m.RowsFailed.Add(float64(len(buffer)))
				return err
			}
			m.InsertDuration.Observe(time.Since(start).Seconds())
			m.RowsIngested.Add(float64(len(buffer)))
			buffer = buffer[:0] // clear buffer after successful insert
		}
		return nil
	})

	if err != nil {
		log.Printf("Consumer stopped: %v", err)
	}
}