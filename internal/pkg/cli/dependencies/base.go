package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// base dependencies container implements Base interface.
type base struct {
	dependencies.Base
	commandCtx context.Context
	fs         filesystem.Fs
	fsInfo     FsInfo
	dialogs    *dialog.Dialogs
	options    *options.Options
	emptyDir   dependencies.Lazy[filesystem.Fs]
}

func newBaseDeps(commandCtx context.Context, envs env.Provider, logger log.Logger, httpClient client.Client, fs filesystem.Fs, dialogs *dialog.Dialogs, opts *options.Options) *base {
	return &base{
		Base:       dependencies.NewBaseDeps(envs, logger, httpClient),
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

func cliHttpClient(logger log.Logger, dumpHttp bool) client.Client {
	c := client.New().
		WithTransport(client.DefaultTransport()).
		WithUserAgent(fmt.Sprintf("keboola-cli/%s", build.BuildVersion))

	// Log each HTTP client request/response as debug message
	// The CLI by default does not display these messages, but they are written always to the log file.
	c = c.AndTrace(client.LogTracer(logger.DebugWriter()))

	// Dump each HTTP client request/response body
	if dumpHttp {
		c = c.AndTrace(client.DumpTracer(logger.DebugWriter()))
	}

	return c
}
