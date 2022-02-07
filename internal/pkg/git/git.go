package git

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

func Available() bool {
	_, err := exec.LookPath("git")
	return err == nil
}

func CheckoutTemplateRepository(ref model.TemplateRef, logger log.Logger) (filesystem.Fs, error) {
	if !Available() {
		return nil, fmt.Errorf("git command is not available, if you want to use templates from a git repository you have to install it first")
	}

	dir, err := ioutil.TempDir("", "keboola-as-code-templates-")
	if err != nil {
		return nil, err
	}

	err, stdErr, exitCode := runGitCommand(logger, dir, []string{"clone", "--branch", ref.Repository().Ref, "--depth=1", "--no-checkout", "--sparse", "--filter=blob:none", "-q", ref.Repository().Url, dir})
	if err != nil {
		if exitCode == 128 {
			if strings.Contains(stdErr, fmt.Sprintf("Remote branch %s not found", ref.Repository().Ref)) {
				return nil, fmt.Errorf(`reference "%s" not found in the templates git repository "%s"`, ref.Repository().Ref, ref.Repository().Url)
			}
			return nil, fmt.Errorf(`templates git repository not found on url "%s"`, ref.Repository().Url)
		}
		return nil, fmt.Errorf(stdErr)
	}

	version := ref.Version()
	templateFolder := fmt.Sprintf("%s/%s/%s", ref.TemplateId(), version.String(), template.SrcDirectory)
	err, stdErr, _ = runGitCommand(logger, dir, []string{"sparse-checkout", "set", fmt.Sprintf("/%s", templateFolder)})
	if err != nil {
		return nil, fmt.Errorf(stdErr)
	}

	err, stdErr, exitCode = runGitCommand(logger, dir, []string{"checkout"})
	if err != nil {
		return nil, fmt.Errorf(stdErr)
	}

	fs, err := aferofs.NewLocalFs(logger, dir, "")
	if err != nil {
		return nil, err
	}

	if !fs.Exists(templateFolder) {
		return nil, fmt.Errorf(`template "%s" in version "%s" not found in the templates git repository "%s"`, ref.TemplateId(), version.String(), ref.Repository().Url)
	}

	return fs, nil
}

func runGitCommand(logger log.Logger, dir string, args []string) (err error, stdErr string, exitCode int) {
	logger.Debug(fmt.Sprintf(`Running git command: git %s`, strings.Join(args, " ")))
	var stdErrBuffer bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = logger.DebugWriter()
	cmd.Stderr = io.MultiWriter(logger.DebugWriter(), &stdErrBuffer)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0")
	err = cmd.Run()
	stdErr = stdErrBuffer.String()
	exitCode = 0
	if err != nil {
		// nolint: errorlint
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
	}
	return
}
