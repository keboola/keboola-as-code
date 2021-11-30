package testcli

import (
	"os"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd"
	interactivePrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func NewTestRootCommand(fs filesystem.Fs) (*cmd.RootCommand, *utils.Writer) {
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	fsFactory := func(logger *zap.SugaredLogger, workingDir string) (filesystem.Fs, error) {
		return fs, nil
	}
	return cmd.NewRootCommand(in, out, out, nopPrompt.New(), env.Empty(), fsFactory), out
}

func NewTestRootCommandWithTty(tty *os.File, fs filesystem.Fs) *cmd.RootCommand {
	prompt := interactivePrompt.New(tty, tty, tty)
	fsFactory := func(logger *zap.SugaredLogger, workingDir string) (filesystem.Fs, error) {
		return fs, nil
	}
	return cmd.NewRootCommand(tty, tty, tty, prompt, env.Empty(), fsFactory)
}
