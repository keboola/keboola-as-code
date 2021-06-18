package diff

import (
	"go.uber.org/zap"
	"keboola-as-code/src/api"
	"keboola-as-code/src/client"
)

type ResultState int

const (
	ResultNotSet ResultState = iota
	ResultNotEqual
	ResultEqual
	ResultOnlyInRemote
	ResultOnlyInLocal
)

type Result interface {
	State() ResultState
	Changes() []string
	SaveRemote(pool *client.Pool, a *api.StorageApi, logger *zap.SugaredLogger) error
	DeleteRemote(pool *client.Pool, a *api.StorageApi, logger *zap.SugaredLogger) error
	DeleteLocal(logger *zap.SugaredLogger) error
	SaveLocal(logger *zap.SugaredLogger) error
}

type Results struct {
	Results []Result
}

type resultData struct {
	state   ResultState
	changes []string
}

func (d resultData) State() ResultState {
	return d.state
}

func (d resultData) Changes() []string {
	return d.changes
}

type BranchDiff struct {
	resultData
	*BranchState
}
type ConfigDiff struct {
	resultData
	*ConfigState
}
type ConfigRowDiff struct {
	resultData
	*ConfigRowState
}
