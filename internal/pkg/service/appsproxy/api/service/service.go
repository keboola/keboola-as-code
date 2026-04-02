package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/gen/apps_proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const e2bWebhookForwardTimeout = 30 * time.Second

type service struct {
	config               config.Config
	deps                 dependencies.ServiceScope
	e2bWebhookHTTPClient *http.Client
}

func New(ctx context.Context, d dependencies.ServiceScope) (Service, error) {
	s := &service{
		config: d.Config(),
		deps:   d,
		e2bWebhookHTTPClient: &http.Client{
			Transport: d.UpstreamTransport(),
			Timeout:   e2bWebhookForwardTimeout,
		},
	}

	return s, nil
}

func (s *service) APIRootIndex(context.Context, dependencies.PublicRequestScope) error {
	// Redirect /_proxy/api -> /_proxy/api/v1
	return nil
}

func (s *service) APIVersionIndex(context.Context, dependencies.PublicRequestScope) (*ServiceDetail, error) {
	url := *s.deps.Config().API.PublicURL
	url.Path = path.Join(url.Path, "v1/documentation")
	res := &ServiceDetail{
		API:           "apps-proxy",
		Documentation: url.String(),
	}
	return res, nil
}

func (s *service) HealthCheck(context.Context, dependencies.PublicRequestScope) (string, error) {
	return "OK", nil
}

func (s *service) Validate(context.Context, dependencies.ProjectRequestScope, *ValidatePayload) (*Validations, error) {
	return nil, nil
}

func (s *service) ForwardE2bWebhook(ctx context.Context, deps dependencies.PublicRequestScope, body io.ReadCloser) error {
	defer body.Close()

	upstreamURL := s.config.E2bWebhook.UpstreamURL
	if upstreamURL == "" {
		return errors.New("E2B webhook forwarding is not configured")
	}

	// Read the body so we can verify the signature before forwarding.
	bodyBytes, err := io.ReadAll(io.LimitReader(body, 1<<20)) // 1 MiB limit
	if err != nil {
		return errors.Errorf("failed to read request body: %w", err)
	}

	// Verify HMAC signature: base64NoPad(SHA-256(secret + body)).
	signature := deps.Request().Header.Get("e2b-signature")
	if !s.verifyE2bSignature(signature, bodyBytes) {
		return errors.New("invalid or missing e2b-signature")
	}

	ctx, cancel := context.WithTimeoutCause(ctx, e2bWebhookForwardTimeout, errors.New("forwarding E2B webhook timed out"))
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return errors.Errorf("failed to create upstream request: %w", err)
	}

	// Forward all e2b-* headers from the original request unchanged.
	// This includes e2b-signature, e2b-webhook-id, e2b-delivery-id, e2b-signature-version.
	originalReq := deps.Request()
	for name, values := range originalReq.Header {
		if len(name) > 4 && strings.EqualFold(name[:4], "e2b-") {
			for _, v := range values {
				req.Header.Add(name, v)
			}
		}
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.e2bWebhookHTTPClient.Do(req)
	if err != nil {
		return errors.Errorf("failed to forward webhook to operator: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return errors.Errorf("operator returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// verifyE2bSignature verifies the E2B webhook HMAC signature.
// Algorithm: base64NoPad(SHA-256(secret + body)).
func (s *service) verifyE2bSignature(signature string, body []byte) bool {
	secret := s.config.E2bWebhook.SignatureSecret
	if secret == "" || signature == "" {
		return false
	}

	hash := sha256.New()
	hash.Write([]byte(secret))
	hash.Write(body)
	expected := base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString(hash.Sum(nil))

	return subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) == 1
}
