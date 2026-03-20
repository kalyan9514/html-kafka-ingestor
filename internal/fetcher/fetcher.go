package fetcher

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Fetcher wraps a HTTP client with retry and timeout behaviour.
// Decoupled from the parser so each can be tested and replaced independently.
type Fetcher struct {
	client     *http.Client
	maxRetries int
}

// New creates a Fetcher. Timeout is enforced at the transport level
// so hung connections don't block the pipeline indefinitely.
func New(timeout time.Duration, maxRetries int) *Fetcher {
	return &Fetcher{
		client: &http.Client{
			Timeout: timeout,
		},
		maxRetries: maxRetries,
	}
}

// Here, Fetch retrieves the HTML at the given URL.
// Uses linear backoff between retries to avoid overwhelming a slow or rate-limiting server.
// Returns the full body as a string so the parser stays decoupled from HTTP concerns.
func (f *Fetcher) Fetch(url string) (string, error) {
	var lastErr error

	for attempt := 1; attempt <= f.maxRetries; attempt++ {
		log.Printf("Fetching URL (attempt %d/%d): %s", attempt, f.maxRetries, url)

		resp, err := f.client.Get(url)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			log.Printf("Attempt %d failed: %v", attempt, lastErr)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			log.Printf("Attempt %d failed: %v", attempt, lastErr)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", err)
			log.Printf("Attempt %d failed: %v", attempt, lastErr)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		log.Printf("Successfully fetched %d bytes from %s", len(body), url)
		return string(body), nil
	}

	return "", fmt.Errorf("all %d attempts failed, last error: %w", f.maxRetries, lastErr)
}