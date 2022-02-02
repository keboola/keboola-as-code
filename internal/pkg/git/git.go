package git

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

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

	cmd := exec.Command("git", "clone", "--no-checkout", url, dir)
	cmd.Stdout = logger.DebugWriter()
	cmd.Stderr = logger.DebugWriter()
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0")
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf(`git repository not found on url "%s"`, url)
	}

	cmd = exec.Command("git", "checkout", ref)
	cmd.Dir = dir
	cmd.Stdout = logger.DebugWriter()
	cmd.Stderr = logger.DebugWriter()
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf(`branch "%s" not found in the repository`, ref)
	}

	return aferofs.NewLocalFs(logger, dir, "")
}
