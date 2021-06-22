package recipe

import (
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/log"
	"sort"
)

func (r *Recipe) LogInfo(logger *zap.SugaredLogger) *Recipe {
	return r.Log(log.ToInfoWriter(logger))
}

func (r *Recipe) LogDebug(logger *zap.SugaredLogger) *Recipe {
	return r.Log(log.ToDebugWriter(logger))
}

func (r *Recipe) Log(writer *log.WriteCloser) *Recipe {
	writer.WriteStringNoErr(fmt.Sprintf("Plan for \"%s\" operation:", r.Name))
	actions := r.Actions
	sort.SliceStable(actions, func(i, j int) bool {
		return actions[i].RelativePath() < actions[j].RelativePath()
	})

	if len(actions) == 0 {
		writer.WriteStringNoErr("  no difference")
	} else {
		for _, action := range actions {
			writer.WriteStringNoErr(action.String())
		}
	}

	return r
}
