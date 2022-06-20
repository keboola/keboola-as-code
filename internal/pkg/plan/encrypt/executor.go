package encrypt

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/encryptionapi"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type executor struct {
	*Plan
	ctx                 context.Context
	projectID           int
	logger              log.Logger
	encryptionApiClient client.Sender
	uow                 *local.UnitOfWork
	errors              *utils.MultiError
}

func newExecutor(ctx context.Context, projectID int, logger log.Logger, encryptionApiClient client.Sender, state *state.State, plan *Plan) *executor {
	return &executor{
		Plan:                plan,
		ctx:                 ctx,
		projectID:           projectID,
		logger:              logger,
		encryptionApiClient: encryptionApiClient,
		uow:                 state.LocalManager().NewUnitOfWork(ctx),
		errors:              utils.NewMultiError(),
	}
}

func (e *executor) invoke() error {
	// Encrypt values
	wg := client.NewWaitGroup(e.ctx, e.encryptionApiClient)
	for _, action := range e.actions {
		wg.Send(e.encryptRequest(action))
	}
	if err := wg.Wait(); err != nil {
		e.errors.Append(err)
	}

	// Save changed files
	if err := e.uow.Invoke(); err != nil {
		e.errors.Append(err)
	}

	return e.errors.ErrorOrNil()
}

func (e *executor) encryptRequest(action *action) client.Sendable {
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
	return encryptionapi.
		EncryptRequest(e.projectID, object.GetComponentId(), data).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, results *map[string]string) error {
			for key, encrypted := range *results {
				path := keyToPath[key]
				if err := object.GetContent().SetNestedPath(path, encrypted); err != nil {
					panic(err)
				}
				e.logger.Debugf(`Encrypted "%s:%s"`, object.Desc(), path.String())
			}

			// Save changes
			e.uow.SaveObject(action.ObjectState, action.object, model.NewChangedFields("configuration"))
			return nil
		})
}
