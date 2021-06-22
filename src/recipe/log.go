package recipe

import (
	"go.uber.org/zap"
	"sort"
)

func (r *Recipe) Log(logger *zap.SugaredLogger) *Recipe {
	logger.Debugf("Recipe for \"%s\" operation:", r.Name)
	actions := r.Actions
	sort.SliceStable(actions, func(i, j int) bool {
		return actions[i].RelativePath() < actions[j].RelativePath()
	})
	for _, action := range actions {
		logger.Debugf(action.String())
	}
	return r
}
