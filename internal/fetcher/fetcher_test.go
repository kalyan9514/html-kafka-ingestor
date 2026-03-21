package fetcher

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestFetchSuccess verifies that a successful HTTP response returns the body correctly.
func TestFetchSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("<html><table></table></html>"))
	}))
	defer server.Close()

	f := New(10*time.Second, 3)
	body, err := f.Fetch(server.URL)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if body != "<html><table></table></html>" {
		t.Errorf("unexpected body: %q", body)
	}
}

// TestFetchRetiesOn503 verifies that the fetcher retries on server errors and eventually succeeds.
func TestFetchRetriesOn503(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable) // fail first 2 attempts
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	}))
	defer server.Close()

	f := New(10*time.Second, 3)
	body, err := f.Fetch(server.URL)

	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if body != "success" {
		t.Errorf("unexpected body: %q", body)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

// TestFetchFailsAfterMaxRetries verifies that the fetcher gives up after exhausting all retries.
func TestFetchFailsAfterMaxRetries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	f := New(10*time.Second, 3)
	_, err := f.Fetch(server.URL)

	if err == nil {
		t.Fatal("expected error after max retries, got nil")
	}
}