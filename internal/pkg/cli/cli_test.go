package cli

import (
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/interaction"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func newTestRootCommand() (*rootCommand, *utils.Writer) {
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	return NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out), aferofs.NewMemoryFs), out
}

func newTestRootCommandWithTty(tty *os.File) *rootCommand {
	prompt := interaction.NewPrompt(tty, tty, tty)
	prompt.Interactive = true
	return NewRootCommand(tty, tty, tty, prompt, aferofs.NewMemoryFs)
}
