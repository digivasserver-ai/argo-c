package webhook

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Case 1: 200 OK — webhook found; no creation triggered
// ---------------------------------------------------------------------------
func TestReconciler_ExistingWebhook_Idempotent(t *testing.T) {
	var createCalled int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":456,"active":true,"config":{"url":"https://example.com/hook"}}]`))
			return
		}
		if r.Method == http.MethodPost {
			atomic.AddInt32(&createCalled, 1)
			w.WriteHeader(http.StatusCreated)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	reconciler := NewReconciler(client, nil)

	desired := &Webhook{
		Active: true,
		Config: WebhookConfig{URL: "https://example.com/hook"},
	}

	hook, err := reconciler.Reconcile(context.Background(), "owner", "repo", desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hook == nil || hook.ID != 456 {
		t.Fatalf("expected existing hook ID 456, got: %+v", hook)
	}
	if atomic.LoadInt32(&createCalled) != 0 {
		t.Fatalf("CreateWebhook should NOT be called when webhook already exists")
	}
}

// ---------------------------------------------------------------------------
// Case 2: 404 Not Found — webhook missing; triggers creation
// ---------------------------------------------------------------------------
func TestReconciler_NotFound_CreatesWebhook(t *testing.T) {
	var createCalled int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Not Found"}`))
			return
		}
		if r.Method == http.MethodPost {
			atomic.AddInt32(&createCalled, 1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":123,"active":true,"config":{"url":"https://example.com/hook"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	reconciler := NewReconciler(client, nil)

	desired := &Webhook{
		Active: true,
		Events: []string{"push"},
		Config: WebhookConfig{URL: "https://example.com/hook"},
	}

	hook, err := reconciler.Reconcile(context.Background(), "owner", "repo", desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hook == nil || hook.ID != 123 {
		t.Fatalf("expected hook ID 123, got: %+v", hook)
	}
	if atomic.LoadInt32(&createCalled) != 1 {
		t.Fatalf("expected CreateWebhook to be called once, got %d", createCalled)
	}
}

// ---------------------------------------------------------------------------
// Case 3: 500/503 Server Error — no creation attempted; state untouched
// ---------------------------------------------------------------------------
func TestReconciler_ServerError_HaltsWithoutCreation(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
	}{
		{"InternalServerError", http.StatusInternalServerError},
		{"BadGateway", http.StatusBadGateway},
		{"ServiceUnavailable", http.StatusServiceUnavailable},
		{"GatewayTimeout", http.StatusGatewayTimeout},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var createCalled int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.statusCode)
					_, _ = w.Write([]byte(`{"message":"GitHub error"}`))
					return
				}
				if r.Method == http.MethodPost {
					atomic.AddInt32(&createCalled, 1)
					w.WriteHeader(http.StatusCreated)
					return
				}
			}))
			defer server.Close()

			client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
			reconciler := NewReconciler(client, nil)

			desired := &Webhook{
				Active: true,
				Config: WebhookConfig{URL: "https://example.com/hook"},
			}

			_, err := reconciler.Reconcile(context.Background(), "owner", "repo", desired)
			if err == nil {
				t.Fatalf("expected error for status %d, got nil", tc.statusCode)
			}
			if !errors.Is(err, ErrServerError) {
				t.Fatalf("expected ErrServerError, got: %v", err)
			}
			if atomic.LoadInt32(&createCalled) != 0 {
				t.Fatalf("CreateWebhook should NOT be called on server error %d", tc.statusCode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Case 4: 429 Rate Limited — webhook not marked as missing; rate limit
//           error returned with RetryAfter/ResetAt from headers
// ---------------------------------------------------------------------------
func TestReconciler_RateLimited_HaltsWithoutCreation(t *testing.T) {
	var createCalled int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "30")
			w.Header().Set("X-RateLimit-Reset", "1700000060")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded"}`))
			return
		}
		if r.Method == http.MethodPost {
			atomic.AddInt32(&createCalled, 1)
			w.WriteHeader(http.StatusCreated)
			return
		}
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	reconciler := NewReconciler(client, nil)

	desired := &Webhook{
		Active: true,
		Config: WebhookConfig{URL: "https://example.com/hook"},
	}

	_, err := reconciler.Reconcile(context.Background(), "owner", "repo", desired)
	if err == nil {
		t.Fatalf("expected rate limit error, got nil")
	}
	if !IsRateLimited(err) {
		t.Fatalf("expected IsRateLimited=true, got: %v", err)
	}

	// Verify the RateLimitError carries parsed header values
	var rlErr *RateLimitError
	if !errors.As(err, &rlErr) {
		t.Fatalf("expected *RateLimitError in chain, got: %T", err)
	}
	if rlErr.RetryAfter != 30*time.Second {
		t.Fatalf("expected RetryAfter=30s, got: %v", rlErr.RetryAfter)
	}
	expectedReset := time.Unix(1700000060, 0)
	if !rlErr.ResetAt.Equal(expectedReset) {
		t.Fatalf("expected ResetAt=%v, got: %v", expectedReset, rlErr.ResetAt)
	}

	if atomic.LoadInt32(&createCalled) != 0 {
		t.Fatalf("CreateWebhook should NOT be called on 429 Rate Limit")
	}
}

// ---------------------------------------------------------------------------
// Case 5: 401/403 Auth Failure — fail fast with diagnostic message
// ---------------------------------------------------------------------------
func TestReconciler_AuthErrors_HaltsWithoutCreation(t *testing.T) {
	testCases := []struct {
		name        string
		statusCode  int
		expectedErr error
	}{
		{"Unauthorized", http.StatusUnauthorized, ErrUnauthorized},
		{"Forbidden", http.StatusForbidden, ErrForbidden},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var createCalled int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.statusCode)
					_, _ = w.Write([]byte(`{"message":"Auth error"}`))
					return
				}
				if r.Method == http.MethodPost {
					atomic.AddInt32(&createCalled, 1)
					w.WriteHeader(http.StatusCreated)
					return
				}
			}))
			defer server.Close()

			client := NewHTTPGitHubClient(server.URL, "invalid-token", server.Client())
			reconciler := NewReconciler(client, nil)

			desired := &Webhook{
				Active: true,
				Config: WebhookConfig{URL: "https://example.com/hook"},
			}

			_, err := reconciler.Reconcile(context.Background(), "owner", "repo", desired)
			if err == nil {
				t.Fatalf("expected auth error for %s, got nil", tc.name)
			}
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected %v, got: %v", tc.expectedErr, err)
			}
			if !IsAuthError(err) {
				t.Fatalf("IsAuthError should return true for %s", tc.name)
			}
			if atomic.LoadInt32(&createCalled) != 0 {
				t.Fatalf("CreateWebhook should NOT be called on auth error %d", tc.statusCode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Case 6: Network Timeout / EOF — transport error, hook not assumed absent
// ---------------------------------------------------------------------------
func TestReconciler_Timeout_HaltsWithoutCreation(t *testing.T) {
	var createCalled int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// Simulate a slow server that exceeds the context deadline
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
			return
		}
		if r.Method == http.MethodPost {
			atomic.AddInt32(&createCalled, 1)
			w.WriteHeader(http.StatusCreated)
			return
		}
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	reconciler := NewReconciler(client, nil)

	desired := &Webhook{
		Active: true,
		Config: WebhookConfig{URL: "https://example.com/hook"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := reconciler.Reconcile(ctx, "owner", "repo", desired)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	// Must NOT be treated as "not found"
	if IsNotFound(err) {
		t.Fatalf("timeout must not be interpreted as webhook-not-found")
	}
	if atomic.LoadInt32(&createCalled) != 0 {
		t.Fatalf("CreateWebhook should NOT be called on timeout error")
	}
}

// ---------------------------------------------------------------------------
// Edge: 200 OK with empty list — webhook absent from list, triggers creation
// ---------------------------------------------------------------------------
func TestReconciler_EmptyList_CreatesWebhook(t *testing.T) {
	var createCalled int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
			return
		}
		if r.Method == http.MethodPost {
			atomic.AddInt32(&createCalled, 1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":789,"active":true,"config":{"url":"https://example.com/hook"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	reconciler := NewReconciler(client, nil)

	desired := &Webhook{
		Active: true,
		Config: WebhookConfig{URL: "https://example.com/hook"},
	}

	hook, err := reconciler.Reconcile(context.Background(), "owner", "repo", desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hook == nil || hook.ID != 789 {
		t.Fatalf("expected hook ID 789, got: %+v", hook)
	}
	if atomic.LoadInt32(&createCalled) != 1 {
		t.Fatalf("CreateWebhook should be called once for empty list, got %d", createCalled)
	}
}

// ---------------------------------------------------------------------------
// Edge: FindWebhook helper
// ---------------------------------------------------------------------------
func TestFindWebhook_Found(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[
			{"id":1,"active":true,"config":{"url":"https://a.example.com/hook"}},
			{"id":2,"active":true,"config":{"url":"https://b.example.com/hook"}}
		]`))
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	hook, found, err := FindWebhook(context.Background(), client, "owner", "repo", "https://b.example.com/hook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found || hook == nil || hook.ID != 2 {
		t.Fatalf("expected to find webhook ID 2, got: found=%v hook=%+v", found, hook)
	}
}

func TestFindWebhook_NotFound_EmptyList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	hook, found, err := FindWebhook(context.Background(), client, "owner", "repo", "https://missing.example.com/hook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatalf("expected not found, got: %+v", hook)
	}
}

func TestFindWebhook_PropagatesAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"Server error"}`))
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	_, _, err := FindWebhook(context.Background(), client, "owner", "repo", "https://example.com/hook")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrServerError) {
		t.Fatalf("expected ErrServerError, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Edge: Rate limit header defaults when headers are absent
// ---------------------------------------------------------------------------
func TestReconciler_RateLimited_DefaultsWhenHeadersMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// 429 but no Retry-After or X-RateLimit-Reset headers
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"rate limited"}`))
			return
		}
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	reconciler := NewReconciler(client, nil)

	desired := &Webhook{
		Active: true,
		Config: WebhookConfig{URL: "https://example.com/hook"},
	}

	_, err := reconciler.Reconcile(context.Background(), "owner", "repo", desired)
	if err == nil {
		t.Fatalf("expected rate limit error, got nil")
	}

	var rlErr *RateLimitError
	if !errors.As(err, &rlErr) {
		t.Fatalf("expected *RateLimitError, got: %T", err)
	}
	// Should default to 60s when no Retry-After header present
	if rlErr.RetryAfter != 60*time.Second {
		t.Fatalf("expected default RetryAfter=60s, got: %v", rlErr.RetryAfter)
	}
}

// ---------------------------------------------------------------------------
// Edge: Concurrent Reconcile calls don't race
// ---------------------------------------------------------------------------
func TestReconciler_ConcurrentAccess_NoRace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := NewHTTPGitHubClient(server.URL, "test-token", server.Client())
	reconciler := NewReconciler(client, nil)

	desired := &Webhook{
		Active: true,
		Config: WebhookConfig{URL: "https://example.com/hook"},
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = reconciler.Reconcile(context.Background(), "owner", "repo", desired)
		}()
	}
	wg.Wait()
}
