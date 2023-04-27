package dependencies

import (
	"context"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// base dependencies container implements Base interface.
type base struct {
	dependencies.Base
	commandCtx      context.Context
	fs              filesystem.Fs
	fsInfo          FsInfo
	dialogs         *dialog.Dialogs
	options         *options.Options
	emptyDir        dependencies.Lazy[filesystem.Fs]
	localDbtProject dependencies.Lazy[dbtProjectValue]
}

type dbtProjectValue struct {
	found bool
	value *dbt.Project
}

func newBaseDeps(commandCtx context.Context, envs env.Provider, logger log.Logger, httpClient client.Client, fs filesystem.Fs, dialogs *dialog.Dialogs, opts *options.Options) *base {
	return &base{
		Base:       dependencies.NewBaseDeps(envs, logger, nil, httpClient),
		commandCtx: commandCtx,
		fs:         fs,
		fsInfo:     FsInfo{fs: fs},
		dialogs:    dialogs,
		options:    opts,
	}
}

func (v *base) CommandCtx() context.Context {
	return v.commandCtx
}

func (v *base) Fs() filesystem.Fs {
	return v.fs
}

func (v *base) FsInfo() FsInfo {
	return v.fsInfo
}

func (v *base) Dialogs() *dialog.Dialogs {
	return v.dialogs
}

func (v *base) Options() *options.Options {
	return v.options
}

func (v *base) EmptyDir() (filesystem.Fs, error) {
	return v.emptyDir.InitAndGet(func() (filesystem.Fs, error) {
		if err := v.fsInfo.AssertEmptyDir(); err != nil {
			return nil, err
		}
		return v.fs, nil
	})
}

func (v *base) LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error) {
	value, err := v.localDbtProject.InitAndGet(func() (dbtProjectValue, error) {
		// Get directory
		fs, _, err := v.FsInfo().DbtProjectDir()
		if err != nil {
			return dbtProjectValue{found: false, value: nil}, err
		}

		// Load project
		prj, err := dbt.LoadProject(ctx, fs)
		return dbtProjectValue{found: true, value: prj}, err
	})

	return value.value, value.found, err
}
