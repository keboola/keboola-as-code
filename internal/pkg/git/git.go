package git

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func Available() bool {
	_, err := exec.LookPath("git")
	return err == nil
}

func CheckoutTemplateRepository(url string, ref string, logger log.Logger) (filesystem.Fs, error) {
	if !Available() {
		return nil, fmt.Errorf("git command is not available, if you want to use templates from a git repository you have to install it first")
	}

	dir, err := ioutil.TempDir("", "keboola-as-code-templates-")
	if err != nil {
		return nil, err
	}

	err, stdErr, exitCode := runGitCommand(logger, dir, []string{"clone", "--no-checkout", "-q", url, dir})
	if err != nil {
		if exitCode == 128 {
			return nil, fmt.Errorf(`git repository not found on url "%s"`, url)
		}
		return nil, fmt.Errorf(stdErr)
	}

	err, stdErr, exitCode = runGitCommand(logger, dir, []string{"checkout", ref})
	if err != nil {
		if exitCode == 1 {
			return nil, fmt.Errorf(`branch "%s" not found in the repository`, ref)
		}
		return nil, fmt.Errorf(stdErr)
	}

	return aferofs.NewLocalFs(logger, dir, "")
}

func runGitCommand(logger log.Logger, dir string, args []string) (err error, stdErr string, exitCode int) {
	logger.Debug(fmt.Sprintf(`Running git command: git %s`, strings.Join(args, " ")))
	var stdErrBuffer bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = logger.DebugWriter()
	cmd.Stderr = logger.DebugWriter()
	cmd.Stderr = io.MultiWriter(logger.DebugWriter(), &stdErrBuffer)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0")
	err = cmd.Run()
	stdErr = stdErrBuffer.String()
	exitCode = 0
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			ws := exitError.Sys().(syscall.WaitStatus)
			exitCode = ws.ExitStatus()
		}
	}
	return
}
