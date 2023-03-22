package cli

import (
	"io"

	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/mattn/go-isatty"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
)

func NewPrompt(stdinRaw io.Reader, stdoutRaw io.Writer, stderr io.Writer, forceNonInteractive bool) prompt.Prompt {
	if !forceNonInteractive {
		stdin, ok1 := stdinRaw.(terminal.FileReader)
		stdout, ok2 := stdoutRaw.(terminal.FileWriter)
		if ok1 && ok2 && isatty.IsTerminal(stdin.Fd()) && isatty.IsTerminal(stdout.Fd()) {
			return interactive.New(stdin, stdout, stderr)
		}
	}

	return nop.New()
}
