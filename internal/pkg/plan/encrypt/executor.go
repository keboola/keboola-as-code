package encrypt

import (
	"context"
	"fmt"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type executor struct {
	*Plan
	logger log.Logger
	api    *encryptionapi.Api
	pool   *client.Pool
	uow    *local.UnitOfWork
	errors *utils.MultiError
}

func newExecutor(logger log.Logger, api *encryptionapi.Api, state *state.State, ctx context.Context, plan *Plan) *executor {
	return &executor{
		Plan:   plan,
		logger: logger,
		api:    api,
		pool:   api.NewPool(),
		uow:    state.LocalManager().NewUnitOfWork(ctx),
		errors: utils.NewMultiError(),
	}
}

func (e *executor) invoke() error {
	// Encrypt values
	for _, action := range e.actions {
		e.pool.Request(e.encryptRequest(action)).Send()
	}
	if err := e.pool.StartAndWait(); err != nil {
		e.errors.Append(err)
	}

	// Save changed files
	if err := e.uow.Invoke(); err != nil {
		e.errors.Append(err)
	}

	return e.errors.ErrorOrNil()
}

func (e *executor) encryptRequest(action *action) *client.Request {
	object := action.object

	// Each key for encryption, in the API call, must start with #
	keyToPath := make(map[string]orderedmap.Path)
	data := make(map[string]string)
	for _, unencrypted := range action.values {
		key := `#` + unencrypted.path.String()
		keyToPath[key] = unencrypted.path
		data[key] = unencrypted.value
	}

	// Prepare request
	return e.api.
		CreateEncryptRequest(object.GetComponentId(), data).
		OnSuccess(func(response *client.Response) {
			if !response.HasResult() {
				panic(fmt.Errorf(`missing result of the encrypt API call`))
			}

			// Replace unencrypted values with encrypted
			results := *response.Result().(*map[string]string)
			for key, encrypted := range results {
				path := keyToPath[key]
				if err := object.GetContent().SetNestedPath(path, encrypted); err != nil {
					panic(err)
				}
				e.logger.Debugf(`Encrypted "%s:%s"`, object.Desc(), path.String())
			}

			// Save changes
			e.uow.SaveObject(action.ObjectState, action.object, model.NewChangedFields("configuration"))
		})
}
