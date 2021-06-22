package plan

import (
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/client"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/remote"
)

func SaveRemote(api *remote.StorageApi, result *diff.Result, logger *zap.SugaredLogger, pool *client.Pool) error {
	return fmt.Errorf("TODO REMOTE SAVE")
}

func DeleteRemote(api *remote.StorageApi, result *diff.Result, logger *zap.SugaredLogger, pool *client.Pool) error {
	return fmt.Errorf("TODO REMOTE DELETE")
}
