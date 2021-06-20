package recipe

import "go.uber.org/zap"

func (r *Recipe) Log(logger *zap.SugaredLogger) *Recipe {
	logger.Debugf("Recipe for \"%s\":", r.Name)
	for _, action := range r.Actions {
		logger.Debugf(action.String())
	}
	return r
}
