package diff

import (
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/api"
	"keboola-as-code/src/client"
	"keboola-as-code/src/utils"
)

func (r *Results) ApplyPull(logger *zap.SugaredLogger) error {
	errors := &utils.Error{}
	for _, item := range r.Results {
		switch item.State() {
		case ResultEqual:
			// nop
		case ResultNotEqual:
			err := item.SaveLocal(logger)
			if err != nil {
				errors.Add(err)
			}
		case ResultOnlyInLocal:
			err := item.DeleteLocal(logger)
			if err != nil {
				errors.Add(err)
			}
		case ResultOnlyInRemote:
			err := item.SaveLocal(logger)
			if err != nil {
				errors.Add(err)
			}
		case ResultNotSet:
			panic(fmt.Errorf("diff was not generated"))
		}
	}

	if errors.Len() > 0 {
		return fmt.Errorf("pull failed: %s", errors)
	}

	return nil
}

func (b *BranchDiff) SaveLocal(logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (c *ConfigDiff) SaveLocal(logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (r *ConfigRowDiff) SaveLocal(logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (b *BranchDiff) SaveRemote(pool *client.Pool, a *api.StorageApi, logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (c *ConfigDiff) SaveRemote(pool *client.Pool, a *api.StorageApi, logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (r *ConfigRowDiff) SaveRemote(pool *client.Pool, a *api.StorageApi, logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (b *BranchDiff) DeleteRemote(pool *client.Pool, a *api.StorageApi, logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (c *ConfigDiff) DeleteRemote(pool *client.Pool, a *api.StorageApi, logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (r *ConfigRowDiff) DeleteRemote(pool *client.Pool, a *api.StorageApi, logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (b *BranchDiff) DeleteLocal(logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (c *ConfigDiff) DeleteLocal(logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}

func (r *ConfigRowDiff) DeleteLocal(logger *zap.SugaredLogger) error {
	return fmt.Errorf("TODO")
}
