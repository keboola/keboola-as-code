package git

import (
	"io/ioutil"
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
	dir, err := ioutil.TempDir("", "keboola-as-code-templates-")
	if err != nil {
		return nil, err
	}

	_, err = exec.Command("git", "clone", "--no-checkout", url, dir).Output()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("git", "checkout", ref)
	cmd.Dir = dir
	_, err = cmd.Output()
	if err != nil {
		return nil, err
	}
	return aferofs.NewLocalFs(logger, dir, "")
}
