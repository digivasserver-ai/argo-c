package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrWebhookNotFound indicates that the requested webhook does not exist (HTTP 404).
	ErrWebhookNotFound = errors.New("webhook not found")
	// ErrUnauthorized indicates invalid authentication credentials (HTTP 401).
	ErrUnauthorized = errors.New("github api unauthorized: check token/credentials")
	// ErrForbidden indicates insufficient permissions (HTTP 403).
	ErrForbidden = errors.New("github api forbidden: check repository permissions")
	// ErrRateLimited indicates GitHub API rate limit was exceeded (HTTP 429).
	ErrRateLimited = errors.New("github api rate limited")
	// ErrServerError indicates a GitHub server-side failure (HTTP 5xx).
	ErrServerError = errors.New("github api server error")
)

// RateLimitError is returned when the GitHub API returns HTTP 429.
// It carries the RetryAfter duration and ResetAt time extracted from response
// headers so callers can implement backoff without re-parsing headers.
type RateLimitError struct {
	RetryAfter time.Duration
	ResetAt    time.Time
	Body       string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("%v: retry after %v, reset at %v, body: %s",
		ErrRateLimited, e.RetryAfter, e.ResetAt, e.Body)
}

func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

// IsRateLimited reports whether err (or any error in its chain) is an
// *RateLimitError or wraps ErrRateLimited.
func IsRateLimited(err error) bool {
	if errors.Is(err, ErrRateLimited) {
		return true
	}
	var rl *RateLimitError
	return errors.As(err, &rl)
}

// IsNotFound reports whether err indicates a 404 Not Found.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrWebhookNotFound)
}

// IsAuthError reports whether err indicates a 401 or 403.
func IsAuthError(err error) bool {
	return errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrForbidden)
}

// IsServerError reports whether err indicates a 5xx server failure.
func IsServerError(err error) bool {
	return errors.Is(err, ErrServerError)
}

// WebhookConfig represents the configuration payload for a GitHub webhook.
type WebhookConfig struct {
	URL         string `json:"url"`
	ContentType string `json:"content_type,omitempty"`
	Secret      string `json:"secret,omitempty"`
	InsecureSSL string `json:"insecure_ssl,omitempty"`
}

// Webhook represents a GitHub repository webhook.
type Webhook struct {
	ID     int64         `json:"id,omitempty"`
	Name   string        `json:"name,omitempty"`
	Active bool          `json:"active"`
	Events []string      `json:"events,omitempty"`
	Config WebhookConfig `json:"config"`
}

// GitHubClient defines the client contract for GitHub webhook operations.
type GitHubClient interface {
	GetWebhook(ctx context.Context, owner, repo string, hookID int64) (*Webhook, error)
	ListWebhooks(ctx context.Context, owner, repo string) ([]*Webhook, error)
	CreateWebhook(ctx context.Context, owner, repo string, hook *Webhook) (*Webhook, error)
}

// HTTPGitHubClient implements GitHubClient using standard HTTP requests.
type HTTPGitHubClient struct {
	BaseURL    string
	Token      string
	HTTPClient *http.Client
}

// NewHTTPGitHubClient creates a new HTTP-based GitHub API client.
func NewHTTPGitHubClient(baseURL, token string, httpClient *http.Client) *HTTPGitHubClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	baseURL = strings.TrimRight(baseURL, "/")
	return &HTTPGitHubClient{
		BaseURL:    baseURL,
		Token:      token,
		HTTPClient: httpClient,
	}
}

func (c *HTTPGitHubClient) newRequest(ctx context.Context, method, path string, body interface{}) (*http.Request, error) {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	reqURL := fmt.Sprintf("%s%s", c.BaseURL, path)
	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// parseRateLimitHeaders extracts Retry-After and X-RateLimit-Reset from the
// response. If neither header is present, RetryAfter defaults to 60s and
// ResetAt defaults to time zero.
func parseRateLimitHeaders(resp *http.Response) (retryAfter time.Duration, resetAt time.Time) {
	retryAfter = 60 * time.Second // sensible default

	if ra := resp.Header.Get("Retry-After"); ra != "" {
		if secs, err := strconv.ParseInt(ra, 10, 64); err == nil && secs > 0 {
			retryAfter = time.Duration(secs) * time.Second
		}
	}

	if rst := resp.Header.Get("X-RateLimit-Reset"); rst != "" {
		if epoch, err := strconv.ParseInt(rst, 10, 64); err == nil && epoch > 0 {
			resetAt = time.Unix(epoch, 0)
		}
	}

	return retryAfter, resetAt
}

func (c *HTTPGitHubClient) checkResponseError(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	switch resp.StatusCode {
	case http.StatusNotFound:
		return ErrWebhookNotFound
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: status %d body: %s", ErrUnauthorized, resp.StatusCode, bodyStr)
	case http.StatusForbidden:
		return fmt.Errorf("%w: status %d body: %s", ErrForbidden, resp.StatusCode, bodyStr)
	case http.StatusTooManyRequests:
		retryAfter, resetAt := parseRateLimitHeaders(resp)
		return &RateLimitError{
			RetryAfter: retryAfter,
			ResetAt:    resetAt,
			Body:       bodyStr,
		}
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return fmt.Errorf("%w: status %d body: %s", ErrServerError, resp.StatusCode, bodyStr)
	default:
		return fmt.Errorf("github api error: status %d body: %s", resp.StatusCode, bodyStr)
	}
}

// GetWebhook retrieves a specific webhook by ID.
func (c *HTTPGitHubClient) GetWebhook(ctx context.Context, owner, repo string, hookID int64) (*Webhook, error) {
	path := fmt.Sprintf("/repos/%s/%s/hooks/%d", owner, repo, hookID)
	req, err := c.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error querying github webhook: %w", err)
	}
	defer resp.Body.Close()

	if err := c.checkResponseError(resp); err != nil {
		return nil, err
	}

	var hook Webhook
	if err := json.NewDecoder(resp.Body).Decode(&hook); err != nil {
		return nil, fmt.Errorf("failed to decode webhook response: %w", err)
	}
	return &hook, nil
}

// ListWebhooks retrieves all webhooks for a repository.
func (c *HTTPGitHubClient) ListWebhooks(ctx context.Context, owner, repo string) ([]*Webhook, error) {
	path := fmt.Sprintf("/repos/%s/%s/hooks", owner, repo)
	req, err := c.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error listing github webhooks: %w", err)
	}
	defer resp.Body.Close()

	if err := c.checkResponseError(resp); err != nil {
		return nil, err
	}

	var hooks []*Webhook
	if err := json.NewDecoder(resp.Body).Decode(&hooks); err != nil {
		return nil, fmt.Errorf("failed to decode webhooks response: %w", err)
	}
	return hooks, nil
}

// CreateWebhook creates a new webhook for a repository.
func (c *HTTPGitHubClient) CreateWebhook(ctx context.Context, owner, repo string, hook *Webhook) (*Webhook, error) {
	path := fmt.Sprintf("/repos/%s/%s/hooks", owner, repo)
	req, err := c.newRequest(ctx, http.MethodPost, path, hook)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error creating github webhook: %w", err)
	}
	defer resp.Body.Close()

	if err := c.checkResponseError(resp); err != nil {
		return nil, err
	}

	var created Webhook
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, fmt.Errorf("failed to decode created webhook response: %w", err)
	}
	return &created, nil
}

// FindWebhook searches the list of webhooks for one matching the given URL.
// Returns (webhook, true) if found, (nil, false) otherwise.
// Propagates API errors without treating them as "not found".
func FindWebhook(ctx context.Context, client GitHubClient, owner, repo, url string) (*Webhook, bool, error) {
	hooks, err := client.ListWebhooks(ctx, owner, repo)
	if err != nil {
		return nil, false, err
	}
	for _, h := range hooks {
		if h.Config.URL == url {
			return h, true, nil
		}
	}
	return nil, false, nil
}

// Reconciler manages the lifecycle and reconciliation of GitHub webhooks.
type Reconciler struct {
	client GitHubClient
	logger *log.Logger
}

// NewReconciler creates a new webhook reconciler.
func NewReconciler(client GitHubClient, logger *log.Logger) *Reconciler {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Reconciler{
		client: client,
		logger: logger,
	}
}

// Reconcile ensures the desired webhook exists on the target repository.
//
// Error handling strategy:
//   - 404 Not Found  → webhook is legitimately absent; create it.
//   - 200 OK, empty  → no matching webhook in list; create it.
//   - 200 OK, match  → webhook already exists; return it (idempotent).
//   - 429 Rate Limit → abort immediately; return *RateLimitError with
//     RetryAfter/ResetAt for caller backoff. No state mutation.
//   - 401/403        → fail fast with clear diagnostic. No state mutation.
//   - 5xx / timeout  → abort and bubble the error for retry. No state mutation.
func (r *Reconciler) Reconcile(ctx context.Context, owner, repo string, desired *Webhook) (*Webhook, error) {
	hooks, err := r.client.ListWebhooks(ctx, owner, repo)
	if err != nil {
		if IsNotFound(err) {
			r.logger.Printf("Webhooks endpoint returned 404 for %s/%s; creating new webhook", owner, repo)
			return r.client.CreateWebhook(ctx, owner, repo, desired)
		}

		if IsAuthError(err) {
			r.logger.Printf("Authentication/Authorization failure for %s/%s: %v", owner, repo, err)
			return nil, fmt.Errorf("permission error reconciling webhook for %s/%s: %w", owner, repo, err)
		}

		if IsRateLimited(err) {
			r.logger.Printf("Rate limit encountered while reconciling %s/%s: %v", owner, repo, err)
			return nil, fmt.Errorf("rate limit exceeded reconciling webhook for %s/%s: %w", owner, repo, err)
		}

		if IsServerError(err) {
			r.logger.Printf("GitHub server error encountered while reconciling %s/%s: %v", owner, repo, err)
			return nil, fmt.Errorf("github server error reconciling webhook for %s/%s: %w", owner, repo, err)
		}

		r.logger.Printf("Error fetching webhooks for %s/%s: %v", owner, repo, err)
		return nil, fmt.Errorf("failed to list webhooks for %s/%s: %w", owner, repo, err)
	}

	// Look for an existing webhook with the matching URL
	for _, h := range hooks {
		if h.Config.URL == desired.Config.URL {
			r.logger.Printf("Webhook already exists for %s/%s (ID: %d)", owner, repo, h.ID)
			return h, nil
		}
	}

	// Webhook does not exist in the list; safely create it
	r.logger.Printf("Webhook not found in list for %s/%s; creating new webhook", owner, repo)
	return r.client.CreateWebhook(ctx, owner, repo, desired)
}
