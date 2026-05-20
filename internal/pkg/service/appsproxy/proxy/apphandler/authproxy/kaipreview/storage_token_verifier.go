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
	storageAPIHost string
}

func NewSDKStorageTokenVerifier(storageAPIHost string) *SDKStorageTokenVerifier {
	return &SDKStorageTokenVerifier{storageAPIHost: storageAPIHost}
}

func (v *SDKStorageTokenVerifier) Verify(ctx context.Context, token string) (*StorageTokenVerifyResult, error) {
	// TODO(SDK): NewPublicAPIFromIndex is called with a nil index because
	// VerifyTokenRequest only needs the Storage API base URL ("v2/storage"),
	// which is constructed directly from the host and does not require the index.
	// If the SDK ever changes so that a nil index causes a panic or incorrect
	// base URL construction, this call will break silently — audit when upgrading
	// the keboola-sdk-go dependency.
	publicAPI := keboola.NewPublicAPIFromIndex(v.storageAPIHost, nil)
	result, err := publicAPI.VerifyTokenRequest(token).Send(ctx)
	if err != nil {
		return nil, errors.Errorf("kai-preview: SDK Storage token verify: %w", err)
	}
	if result.Owner.ID == 0 {
		return nil, errors.New("kai-preview: Storage token verify response missing owner.id")
	}
	return &StorageTokenVerifyResult{ProjectID: strconv.Itoa(result.Owner.ID)}, nil
}
