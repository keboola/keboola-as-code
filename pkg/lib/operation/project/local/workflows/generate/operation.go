package generate

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/workflows"
)

type Options struct {
	Validate   bool // validate all branches
	Push       bool // push to the project on change in the main branch
	Pull       bool // periodical pull new changes to the main branch
	MainBranch string
}

func (o Options) Enabled() bool {
	return o.Validate || o.Push || o.Pull
}

type dependencies interface {
	Logger() log.Logger
	LocalProject(ignoreErrors bool) (*project.Project, error)
}

func Run(o Options, d dependencies) (err error) {
	if !o.Enabled() {
		return nil
	}

	prj, err := d.LocalProject(false)
	if err != nil {
		return err
	}

	return workflows.GenerateFiles(d.Logger(), prj.Fs(), &workflows.Options{
		Validate:   o.Validate,
		Push:       o.Push,
		Pull:       o.Pull,
		MainBranch: o.MainBranch,
	})
}
