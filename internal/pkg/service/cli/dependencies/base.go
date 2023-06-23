package dependencies

import (
	"context"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// baseScope dependencies container implements BaseScope interface.
type baseScope struct {
	dependencies.BaseScope
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

func newBaseScope(ctx context.Context, logger log.Logger, proc *servicectx.Process, httpClient client.Client, fs filesystem.Fs, dialogs *dialog.Dialogs, opts *options.Options) *baseScope {
	return &baseScope{
		BaseScope:  dependencies.NewBaseScope(ctx, logger, telemetry.NewNop(), clock.New(), proc, httpClient),
		commandCtx: ctx,
		fs:         fs,
		fsInfo:     FsInfo{fs: fs},
		dialogs:    dialogs,
		options:    opts,
	}
}

func (v *baseScope) CommandCtx() context.Context {
	return v.commandCtx
}

func (v *baseScope) Fs() filesystem.Fs {
	return v.fs
}

func (v *baseScope) FsInfo() FsInfo {
	return v.fsInfo
}

func (v *baseScope) Dialogs() *dialog.Dialogs {
	return v.dialogs
}

func (v *baseScope) Options() *options.Options {
	return v.options
}

func (v *baseScope) EmptyDir() (filesystem.Fs, error) {
	return v.emptyDir.InitAndGet(func() (filesystem.Fs, error) {
		if err := v.fsInfo.AssertEmptyDir(); err != nil {
			return nil, err
		}
		return v.fs, nil
	})
}

func (v *baseScope) LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error) {
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
