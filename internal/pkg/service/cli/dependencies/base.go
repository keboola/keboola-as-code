package dependencies

import (
	"context"
	"io"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/keboola-sdk-go/v2/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmdconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/flag"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// baseScope dependencies container implements BaseScope interface.
type baseScope struct {
	dependencies.BaseScope
	envs            *env.Map
	fs              filesystem.Fs
	fsInfo          FsInfo
	configBinder    *cmdconfig.Binder
	globalsFlags    flag.GlobalFlags
	dialogs         *dialog.Dialogs
	emptyDir        dependencies.Lazy[filesystem.Fs]
	localDbtProject dependencies.Lazy[dbtProjectValue]
}

type dbtProjectValue struct {
	found bool
	value *dbt.Project
}

func newBaseScope(
	ctx context.Context,
	logger log.Logger,
	stdout io.Writer,
	stderr io.Writer,
	proc *servicectx.Process,
	httpClient client.Client,
	fs filesystem.Fs,
	dialogs *dialog.Dialogs,
	flags flag.GlobalFlags,
	envs *env.Map,
) *baseScope {
	return &baseScope{
		BaseScope:    dependencies.NewBaseScope(ctx, logger, telemetry.NewNop(), stdout, stderr, clockwork.NewRealClock(), proc, httpClient),
		envs:         envs,
		fs:           fs,
		fsInfo:       FsInfo{fs: fs},
		configBinder: cmdconfig.NewBinder(envs, logger),
		dialogs:      dialogs,
		globalsFlags: flags,
	}
}

func (v *baseScope) Environment() env.Provider {
	return v.envs
}

func (v *baseScope) Fs() filesystem.Fs {
	return v.fs
}

func (v *baseScope) FsInfo() FsInfo {
	return v.fsInfo
}

func (v *baseScope) ConfigBinder() *cmdconfig.Binder {
	return v.configBinder
}

func (v *baseScope) GlobalFlags() flag.GlobalFlags {
	return v.globalsFlags
}

func (v *baseScope) Dialogs() *dialog.Dialogs {
	return v.dialogs
}

func (v *baseScope) EmptyDir(ctx context.Context) (filesystem.Fs, error) {
	return v.emptyDir.InitAndGet(func() (filesystem.Fs, error) {
		if err := v.fsInfo.AssertEmptyDir(ctx); err != nil {
			return nil, err
		}
		return v.fs, nil
	})
}

func (v *baseScope) LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error) {
	value, err := v.localDbtProject.InitAndGet(func() (dbtProjectValue, error) {
		// Get directory
		fs, _, err := v.FsInfo().DbtProjectDir(ctx)
		if err != nil {
			return dbtProjectValue{found: false, value: nil}, err
		}

		// Load project
		prj, err := dbt.LoadProject(ctx, fs)
		return dbtProjectValue{found: true, value: prj}, err
	})

	return value.value, value.found, err
}
