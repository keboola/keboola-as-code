package plan

import (
	"fmt"

	"go.uber.org/zap"

	"keboola-as-code/src/log"
	"keboola-as-code/src/manifest"
)

type RenamePlan struct {
	actions []*RenameAction
}

func (p *RenamePlan) Empty() bool {
	return len(p.actions) == 0
}

func (p *RenamePlan) Name() string {
	return "rename"
}

func (p *RenamePlan) Log(writer *log.WriteCloser) {
	writer.WriteStringNoErr(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	actions := p.actions

	if len(actions) == 0 {
		writer.WriteStringNoErr("\tno paths to rename")
	} else {
		for _, action := range actions {
			writer.WriteStringNoErr("\t- " + action.String())
		}
	}
}

func (p *RenamePlan) Invoke(logger *zap.SugaredLogger, projectDir string, manifest *manifest.Manifest) (warns error, errs error) {
	return newRenameExecutor(logger, projectDir, manifest, p).invoke()
}
