package kaipreview

import (
	"context"
	"strconv"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// StorageTokenVerifier abstracts SDKStorageTokenVerifier so tests can inject a stub without HTTP.
type StorageTokenVerifier interface {
	Verify(ctx context.Context, token string) (*StorageTokenVerifyResult, error)
}

// StorageTokenVerifyResult is the subset of Storage API's tokens/verify response that the
// kai-preview flow consumes. We deliberately ignore email, name, roles — see
// docs/superpowers/specs/2026-05-14-dev-iframe-auth-design.md "no identity in
// transit".
type StorageTokenVerifyResult struct {
	ProjectID string
}

type SDKStorageTokenVerifier struct {
	publicAPI *keboola.PublicAPI
}

// NewSDKStorageTokenVerifier loads the Storage API index once and reuses the
// resulting PublicAPI client for all token verifications. We use NewPublicAPI
// (not NewPublicAPIFromIndex with nil) so the SDK's documented invariants hold
// for every method on the client, not just token verify.
func NewSDKStorageTokenVerifier(ctx context.Context, storageAPIHost string) (*SDKStorageTokenVerifier, error) {
	publicAPI, err := keboola.NewPublicAPI(ctx, storageAPIHost)
	if err != nil {
		return nil, errors.Errorf("kai-preview: build Storage API client: %w", err)
	}
	return &SDKStorageTokenVerifier{publicAPI: publicAPI}, nil
}

func (v *SDKStorageTokenVerifier) Verify(ctx context.Context, token string) (*StorageTokenVerifyResult, error) {
	result, err := v.publicAPI.VerifyTokenRequest(token).Send(ctx)
	if err != nil {
		return nil, errors.Errorf("kai-preview: SDK Storage token verify: %w", err)
	}
	if result.Owner.ID == 0 {
		return nil, errors.New("kai-preview: Storage token verify response missing owner.id")
	}
	return &StorageTokenVerifyResult{ProjectID: strconv.Itoa(result.Owner.ID)}, nil
}
