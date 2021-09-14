package plan

import (
	"fmt"

	"go.uber.org/zap"

	"keboola-as-code/src/client"
	"keboola-as-code/src/encryption"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

type encryptExecutor struct {
	*EncryptPlan
	*state.State
	projectId int
	logger    *zap.SugaredLogger
	api       *encryption.Api
	errors    *utils.Error
}

func newEncryptExecutor(projectId int, logger *zap.SugaredLogger, api *encryption.Api, projectState *state.State, plan *EncryptPlan) *encryptExecutor {
	return &encryptExecutor{
		EncryptPlan: plan,
		State:       projectState,
		projectId:   projectId,
		logger:      logger,
		api:         api,
		errors:      utils.NewMultiError(),
	}
}

func (e *encryptExecutor) invoke() error {
	pool := e.api.NewPool()
	for _, action := range e.actions {
		pool.Request(e.encryptRequest(action)).Send()
	}

	if err := pool.StartAndWait(); err != nil {
		e.errors.Append(err)
	}

	return e.errors.ErrorOrNil()
}

func (e *encryptExecutor) encryptRequest(action *EncryptAction) *client.Request {
	object := action.object
	manifest := action.manifest

	// Each key for encryption, in the API call, must start with #
	keyToPath := make(map[string]utils.KeyPath)
	data := make(map[string]string)
	for _, unencrypted := range action.values {
		key := `#` + unencrypted.path.String()
		keyToPath[key] = unencrypted.path
		data[key] = unencrypted.value
	}

	// Prepare request
	return e.api.
		CreateEncryptRequest(object.GetComponentId(), e.projectId, data).
		OnSuccess(func(response *client.Response) {
			if !response.HasResult() {
				panic(fmt.Errorf(`missing result of the encrypt API call`))
			}

			// Replace unencrypted values with encrypted
			results := *response.Result().(*map[string]string)
			for key, encrypted := range results {
				path := keyToPath[key]
				utils.UpdateIn(object.GetContent(), path, encrypted)
				e.logger.Debugf(`Encrypted "%s:%s"`, object.Desc(), path.String())
			}

			// Save changes
			if err := e.LocalManager().SaveModel(manifest, object); err == nil {
				e.logger.Debugf(`Saved "%s"`, manifest.RelativePath())
			} else {
				e.errors.Append(err)
			}
		})
}
