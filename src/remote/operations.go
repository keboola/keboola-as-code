package remote

import (
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/client"
	"keboola-as-code/src/diff"
)

func (a *StorageApi) Save(result *diff.Result, logger *zap.SugaredLogger, pool *client.Pool) error {
	return fmt.Errorf("TODO REMOTE SAVE")
}

func (a *StorageApi) Delete(result *diff.Result, logger *zap.SugaredLogger, pool *client.Pool) error {
	return fmt.Errorf("TODO REMOTE DELETE")
}
