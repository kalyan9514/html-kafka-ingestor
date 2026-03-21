package fetcher

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Fetcher wraps an HTTP client with retry and timeout behaviour.
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

// Fetch retrieves the HTML at the given URL.
// Uses linear backoff between retries to avoid overwhelming a slow or rate-limiting server.
// Returns the full body as a string so the parser stays decoupled from HTTP concerns.
func (f *Fetcher) Fetch(url string) (string, error) {
	var lastErr error
	ctx := context.Background()

	for attempt := 1; attempt <= f.maxRetries; attempt++ {
		log.Printf("Fetching URL (attempt %d/%d): %s", attempt, f.maxRetries, url)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			log.Printf("Attempt %d failed: %v", attempt, lastErr)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		// mimic a real browser so servers like Wikipedia don't block us with 403
		req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

		resp, err := f.client.Do(req)
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