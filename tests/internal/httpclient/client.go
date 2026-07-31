// Package httpclient provides a minimal, dependency-free HTTP client that
// speaks the gateway REST API. Every public method mirrors one endpoint in
// apps/gateway/gateway.api and returns the strongly typed response struct.
//
// The client is deliberately thin:
//   - no retries (the caller decides what to retry)
//   - no connection pooling beyond net/http's default transport
//   - Bearer token handled via SetToken; nil means anonymous request
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// Client is a thread-safe wrapper around http.Client bound to a gateway base URL.
// Concurrent callers may issue requests with different bearer tokens by
// passing the token via WithToken(ctx, tok); SetToken installs a default
// for anonymous convenience but is optional.
type Client struct {
	base string
	hc   *http.Client
	// token stored as *string to make atomic swap safe under concurrency.
	token atomic.Pointer[string]
}

// tokenCtxKey scopes per-request bearer tokens on a context.
type tokenCtxKey struct{}

// WithToken returns a derived context carrying the given bearer token; empty
// string means "explicitly anonymous, ignore the client-level default".
func WithToken(ctx context.Context, tok string) context.Context {
	return context.WithValue(ctx, tokenCtxKey{}, tok)
}

// tokenFrom fetches the bearer token to use for a request. Per-context value
// wins over the client-level default; a "" per-context value means anonymous.
func (c *Client) tokenFrom(ctx context.Context) string {
	if v := ctx.Value(tokenCtxKey{}); v != nil {
		return v.(string)
	}
	return c.Token()
}

// New returns a Client with the given base URL and per-request timeout.
// baseURL may or may not end with a slash.
func New(baseURL string, timeout time.Duration) *Client {
	base := strings.TrimRight(baseURL, "/")
	return &Client{
		base: base,
		hc: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        1024,
				MaxIdleConnsPerHost: 512,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// SetToken installs the JWT used for subsequent requests. Empty string clears it.
func (c *Client) SetToken(t string) {
	if t == "" {
		c.token.Store(nil)
		return
	}
	c.token.Store(&t)
}

// Token returns the current bearer token or empty string.
func (c *Client) Token() string {
	p := c.token.Load()
	if p == nil {
		return ""
	}
	return *p
}

// APIError carries the HTTP status and (best-effort) response body when a
// call returns non-2xx. It implements error so callers can errors.As it.
type APIError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *APIError) Error() string {
	body := e.Body
	if len(body) > 256 {
		body = body[:256] + "..."
	}
	return fmt.Sprintf("%s %s -> %d: %s", e.Method, e.Path, e.Status, body)
}

// Do issues an HTTP request with optional JSON body and decodes the JSON
// response into out (when non-nil). Query params are appended if non-empty.
func (c *Client) Do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	fullURL := c.base + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		reader = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if tok := c.tokenFrom(ctx); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return &APIError{
			Method: method,
			Path:   path,
			Status: resp.StatusCode,
			Body:   string(raw),
		}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
