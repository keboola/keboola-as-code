package bridge

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// apiProvider defines how the API instance should be obtained from the context.
type apiProvider func(ctx context.Context) *keboola.AuthorizedAPI

func (fn apiProvider) APIFromContext(ctx context.Context) (*keboola.AuthorizedAPI, error) {
	api := fn(ctx)
	if api == nil {
		return nil, errors.New("keboola.AuthorizedAPI is not present in the context")
	}
	return api, nil
}
