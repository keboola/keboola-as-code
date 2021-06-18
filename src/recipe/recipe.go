package recipe

import (
	"go.uber.org/zap"
	"keboola-as-code/src/diff"
)

type ActionType int

const (
	ActionSaveLocal ActionType = iota
	ActionSaveRemote
	ActionDeleteLocal
	ActionDeleteRemote
)

type Recipe struct {
	Actions []*Action
}

type Action struct {
	*diff.Result
	Type ActionType
}

//SaveRemote(logger *zap.SugaredLogger, workers *errgroup.Group, pool *client.Pool, a *api.StorageApi) error
//DeleteRemote(logger *zap.SugaredLogger, workers *errgroup.Group, pool *client.Pool, a *api.StorageApi) error
//DeleteLocal(logger *zap.SugaredLogger, workers *errgroup.Group) error
//SaveLocal(logger *zap.SugaredLogger, workers *errgroup.Group) error

func (a *Action) String() string {
	return a.StringPrefix() + " " + a.LocalPath()
}

func (a *Action) StringPrefix() string {
	switch a.Result.State {
	case diff.ResultNotSet:
		return "? "
	case diff.ResultNotEqual:
		return "CH"
	case diff.ResultEqual:
		return "= "
	default:
		if a.Type == ActionSaveLocal || a.Type == ActionSaveRemote {
			return "+ "
		} else {
			return "- "
		}
	}
}

func (r *Recipe) Add(d *diff.Result, t ActionType) {
	r.Actions = append(r.Actions, &Action{d, t})
}

func (r *Recipe) Log(logger *zap.SugaredLogger) *Recipe {
	logger.Debugf("Planned actions:")
	for _, action := range r.Actions {
		logger.Debugf(action.String())
	}
	return r
}
