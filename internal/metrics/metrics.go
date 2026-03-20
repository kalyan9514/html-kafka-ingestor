package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus counters and histograms for the pipeline.
type Metrics struct {
	RowsIngested prometheus.Counter
	RowsFailed   prometheus.Counter
	InsertDuration prometheus.Histogram
}

// New registers all metrics with Prometheus and returns them for use in the app.
// Registering here ensures metrics appear in /metrics even before any data flows.
func New() *Metrics {
	m := &Metrics{
		RowsIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "rows_ingested_total",
			Help: "Total number of rows successfully inserted into MySQL",
		}),
		RowsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "rows_failed_total",
			Help: "Total number of rows that failed processing",
		}),
		InsertDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "db_insert_duration_seconds",
			Help:    "Time taken for batch inserts into MySQL",
			Buckets: prometheus.DefBuckets, // default buckets: 5ms to 10s
		}),
	}

	prometheus.MustRegister(m.RowsIngested, m.RowsFailed, m.InsertDuration)
	return m
}

// StartServer exposes the /metrics endpoint on the given port.
// Prometheus scrapes this endpoint every 15 seconds as configured in prometheus.yml.
func StartServer(port string) {
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":"+port, nil)
}