package plan

import (
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/log"
	"sort"
)

func (p *Plan) LogInfo(logger *zap.SugaredLogger) *Plan {
	return p.Log(log.ToInfoWriter(logger))
}

func (p *Plan) LogDebug(logger *zap.SugaredLogger) *Plan {
	return p.Log(log.ToDebugWriter(logger))
}

func (p *Plan) Log(writer *log.WriteCloser) *Plan {
	writer.WriteStringNoErr(fmt.Sprintf("Plan for \"%s\" operation:", p.Name))
	actions := p.Actions
	sort.SliceStable(actions, func(i, j int) bool {
		return actions[i].RelativePath() < actions[j].RelativePath()
	})

	if len(actions) == 0 {
		writer.WriteStringNoErr("  no difference")
	} else {
		skippedDeleteCount := 0
		for _, action := range actions {
			msg := action.String()
			if !p.allowedRemoteDelete && action.Type == ActionDeleteRemote {
				msg += " - SKIPPED"
				skippedDeleteCount++
			}
			writer.WriteStringNoErr(msg)
		}

		if skippedDeleteCount > 0 {
			writer.WriteStringNoErr("Skipped remote objects deletion, use \"--force\" to delete them.")
		}
	}

	return p
}
