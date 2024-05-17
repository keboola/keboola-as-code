package bridge

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// apiProvider defines how the API instance should be obtained from the context.
type apiProvider func(ctx context.Context) *keboola.AuthorizedAPI

func (fn apiProvider) APIFromContext(ctx context.Context) (*keboola.AuthorizedAPI, bool) {
	api := fn(ctx)
	return api, api != nil
}

func (fn apiProvider) MustAPIFromContext(ctx context.Context) *keboola.AuthorizedAPI {
	if api := fn(ctx); api == nil {
		panic(errors.New("keboola.AuthorizedAPI is not present in the context"))
	} else {
		return api
	}
}
