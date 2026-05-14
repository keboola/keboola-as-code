package kaipreview

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// STAVerifyResult is the subset of Storage API's tokens/verify response that the
// kai-preview flow consumes. We deliberately ignore email, name, roles — see
// docs/superpowers/specs/2026-05-14-dev-iframe-auth-design.md "no identity in
// transit".
type STAVerifyResult struct {
	ProjectID string
}

type STAVerifier struct {
	baseURL string
	client  *http.Client
}

func NewSTAVerifier(baseURL string, client *http.Client) *STAVerifier {
	return &STAVerifier{baseURL: baseURL, client: client}
}

func (v *STAVerifier) Verify(ctx context.Context, token string) (*STAVerifyResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.baseURL+"/v2/storage/tokens/verify", nil)
	if err != nil {
		return nil, errors.Errorf("kai-preview: build STA verify request: %w", err)
	}
	req.Header.Set("X-StorageApi-Token", token)
	req.Header.Set("Accept", "application/json")

	resp, err := v.client.Do(req)
	if err != nil {
		return nil, errors.Errorf("kai-preview: STA verify call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, errors.New("kai-preview: STA token unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("kai-preview: STA verify returned %d", resp.StatusCode)
	}

	var body struct {
		Owner struct {
			ID string `json:"id"`
		} `json:"owner"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, errors.Errorf("kai-preview: decode STA verify response: %w", err)
	}
	if body.Owner.ID == "" {
		return nil, errors.New("kai-preview: STA verify response missing owner.id")
	}
	return &STAVerifyResult{ProjectID: body.Owner.ID}, nil
}
