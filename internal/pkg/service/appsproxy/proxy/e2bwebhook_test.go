package proxy_test

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

const testE2bSecret = "test-secret-that-is-at-least-32-chars!!"

// computeE2bSignature computes e2b-signature for testing: base64NoPad(SHA-256(secret + body)).
func computeE2bSignature(secret string, body []byte) string {
	hash := sha256.New()
	hash.Write([]byte(secret))
	hash.Write(body)
	return base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString(hash.Sum(nil))
}

func TestForwardE2bWebhook(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Track what the fake operator received.
	var mu sync.Mutex
	var receivedBody string
	var receivedHeaders http.Header

	// Start a fake operator webhook server.
	operatorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		body, _ := io.ReadAll(r.Body)
		receivedBody = string(body)
		receivedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer operatorServer.Close()

	// Configure proxy with E2B webhook upstream pointing to fake operator.
	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CsrfTokenSalt = "abc"
	cfg.E2bWebhook.UpstreamURL = operatorServer.URL
	cfg.E2bWebhook.SignatureSecret = testE2bSecret

	d, _ := proxyDependencies.NewMockedServiceScope(t, ctx, cfg, dependencies.WithRealHTTPClient())

	// Create proxy handler.
	handler := proxy.NewHandler(ctx, d)

	// Build a webhook request with valid signature and all e2b-* headers.
	webhookBody := `{"version":"v2","id":"evt-1","type":"sandbox.lifecycle.killed","sandboxId":"sb-123","sandboxTeamId":"team-1","sandboxTemplateId":"tmpl-1","timestamp":"2025-08-06T20:59:24Z"}`
	signature := computeE2bSignature(testE2bSecret, []byte(webhookBody))

	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/_proxy/api/v1/e2b-webhook", strings.NewReader(webhookBody))
	req.Host = "hub.keboola.local"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("e2b-signature", signature)
	req.Header.Set("e2b-webhook-id", "wh-456")
	req.Header.Set("e2b-delivery-id", "del-789")
	req.Header.Set("e2b-signature-version", "v1")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Verify response.
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the fake operator received the correct body.
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, webhookBody, receivedBody)

	// Verify all e2b-* headers were forwarded.
	assert.Equal(t, signature, receivedHeaders.Get("e2b-signature"))
	assert.Equal(t, "wh-456", receivedHeaders.Get("e2b-webhook-id"))
	assert.Equal(t, "del-789", receivedHeaders.Get("e2b-delivery-id"))
	assert.Equal(t, "v1", receivedHeaders.Get("e2b-signature-version"))
	assert.Equal(t, "application/json", receivedHeaders.Get("Content-Type"))
}

func TestForwardE2bWebhookInvalidSignature(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Start a fake operator that should NOT be called.
	operatorCalled := false
	operatorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operatorCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer operatorServer.Close()

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CsrfTokenSalt = "abc"
	cfg.E2bWebhook.UpstreamURL = operatorServer.URL
	cfg.E2bWebhook.SignatureSecret = testE2bSecret

	d, _ := proxyDependencies.NewMockedServiceScope(t, ctx, cfg, dependencies.WithRealHTTPClient())

	handler := proxy.NewHandler(ctx, d)

	// Send request with WRONG signature.
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/_proxy/api/v1/e2b-webhook", strings.NewReader(`{"sandboxId":"sb-1"}`))
	req.Host = "hub.keboola.local"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("e2b-signature", "invalid-signature")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.NotEqual(t, http.StatusOK, rec.Code)
	assert.False(t, operatorCalled, "request with invalid signature should not be forwarded")
}

func TestForwardE2bWebhookMissingSignature(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	operatorCalled := false
	operatorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operatorCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer operatorServer.Close()

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CsrfTokenSalt = "abc"
	cfg.E2bWebhook.UpstreamURL = operatorServer.URL
	cfg.E2bWebhook.SignatureSecret = testE2bSecret

	d, _ := proxyDependencies.NewMockedServiceScope(t, ctx, cfg, dependencies.WithRealHTTPClient())

	handler := proxy.NewHandler(ctx, d)

	// Send request WITHOUT e2b-signature header.
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/_proxy/api/v1/e2b-webhook", strings.NewReader(`{}`))
	req.Host = "hub.keboola.local"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.NotEqual(t, http.StatusOK, rec.Code)
	assert.False(t, operatorCalled, "request without e2b-signature should not be forwarded")
}

func TestForwardE2bWebhookDisabled(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Configure proxy WITHOUT E2B webhook upstream (disabled).
	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CsrfTokenSalt = "abc"
	// cfg.E2bWebhook.UpstreamURL is empty — forwarding disabled.

	d, _ := proxyDependencies.NewMockedServiceScope(t, ctx, cfg, dependencies.WithRealHTTPClient())

	handler := proxy.NewHandler(ctx, d)

	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/_proxy/api/v1/e2b-webhook", strings.NewReader(`{}`))
	req.Host = "hub.keboola.local"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestForwardE2bWebhookUpstreamError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Start a fake operator that returns 401.
	operatorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
	}))
	defer operatorServer.Close()

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CsrfTokenSalt = "abc"
	cfg.E2bWebhook.UpstreamURL = operatorServer.URL
	cfg.E2bWebhook.SignatureSecret = testE2bSecret

	d, _ := proxyDependencies.NewMockedServiceScope(t, ctx, cfg, dependencies.WithRealHTTPClient())

	handler := proxy.NewHandler(ctx, d)

	// Send request with valid signature but operator returns error.
	body := `{}`
	signature := computeE2bSignature(testE2bSecret, []byte(body))
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/_proxy/api/v1/e2b-webhook", strings.NewReader(body))
	req.Host = "hub.keboola.local"
	req.Header.Set("e2b-signature", signature)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.NotEqual(t, http.StatusOK, rec.Code)
}
