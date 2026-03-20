package api

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

// Job represents a single ingestion run triggered via the API.
type Job struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"`  // "running", "completed", "failed"
	RowCount  int       `json:"row_count"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Error     string    `json:"error,omitempty"` // only shown if status is "failed"
}

// IngestFunc is the function the API calls to trigger a pipeline run.
// Keeping it as a callback decouples the API from the pipeline implementation.
type IngestFunc func(url string, jobID string) error

// Server holds the API state — job history and the ingest trigger function.
type Server struct {
	mu       sync.RWMutex   // protects jobs map from concurrent reads/writes
	jobs     map[string]*Job
	ingestFn IngestFunc
}

// New creates an API server with the given ingest trigger function.
func New(ingestFn IngestFunc) *Server {
	return &Server{
		jobs:     make(map[string]*Job),
		ingestFn: ingestFn,
	}
}

// Start registers routes and begins listening on the given port.
func (s *Server) Start(port string) {
	http.HandleFunc("/ingest", s.handleIngest)
	http.HandleFunc("/jobs", s.handleJobs)
	log.Printf("API server listening on :%s", port)
	go http.ListenAndServe(":"+port, nil)
}

// handleIngest accepts POST /ingest?url=<url> and triggers a pipeline run.
// The job runs asynchronously so the caller gets an immediate response.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	url := r.URL.Query().Get("url")
	if url == "" {
		http.Error(w, "url query parameter is required", http.StatusBadRequest)
		return
	}

	job := &Job{
		ID:        generateID(),
		URL:       url,
		Status:    "running",
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	s.mu.Lock()
	s.jobs[job.ID] = job
	s.mu.Unlock()

	// run the pipeline in a goroutine so the API responds immediately
	go func() {
		err := s.ingestFn(url, job.ID)
		s.mu.Lock()
		defer s.mu.Unlock()
		job.UpdatedAt = time.Now()
		if err != nil {
			job.Status = "failed"
			job.Error = err.Error()
			log.Printf("Job %s failed: %v", job.ID, err)
		} else {
			job.Status = "completed"
			log.Printf("Job %s completed: %d rows", job.ID, job.RowCount)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted) // 202 — accepted but not yet complete
	json.NewEncoder(w).Encode(job)
}

// handleJobs accepts GET /jobs and returns all ingestion runs.
func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// generateID creates a simple unique job ID based on timestamp.
func generateID() string {
	return time.Now().Format("20060102150405.000000")
}