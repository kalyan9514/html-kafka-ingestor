package main

import (
	"context"
	"log"
	"time"

	"github.com/kalyan9514/html-kafka-ingestor/config"
	"github.com/kalyan9514/html-kafka-ingestor/internal/fetcher"
	"github.com/kalyan9514/html-kafka-ingestor/internal/kafka"
	"github.com/kalyan9514/html-kafka-ingestor/internal/metrics"
	"github.com/kalyan9514/html-kafka-ingestor/internal/parser"
)

func main() {
	cfg := config.Load()

	// start metrics server so Prometheus can scrape producer health
	metrics.StartServer(cfg.MetricsPort)

	f := fetcher.New(30*time.Second, 3)
	html, err := f.Fetch(cfg.TargetURL)
	if err != nil {
		log.Fatalf("Failed to fetch URL: %v", err)
	}

	table, err := parser.Parse(html)
	if err != nil || table == nil {
		log.Fatalf("Failed to parse table: %v", err)
	}

	log.Printf("Parsed table with %d columns and %d rows", len(table.Columns), len(table.Rows))

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer producer.Close()

	ctx := context.Background()
	for i, row := range table.Rows {
		if err := producer.PublishRow(ctx, i, row); err != nil {
			log.Printf("Failed to publish row %d: %v", i, err)
		}
	}

	log.Printf("Producer finished — published %d rows to Kafka topic %s", len(table.Rows), cfg.KafkaTopic)
}