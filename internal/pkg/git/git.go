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
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func Available() bool {
	_, err := exec.LookPath("git")
	return err == nil
}

func CheckoutTemplateRepositoryFull(repo model.TemplateRepository, logger log.Logger) (filesystem.Fs, string, error) {
	if !Available() {
		return nil, "", fmt.Errorf("git command is not available, if you want to use templates from a git repository you have to install it first")
	}

	// Create a temp dir
	dir, err := ioutil.TempDir("", "keboola-as-code-templates-")
	if err != nil {
		return nil, "", err
	}

	err, stdErr, exitCode := runGitCommand(logger, dir, []string{"clone", "--branch", repo.Ref, "-q", repo.Url, dir})
	if err != nil {
		if exitCode == 128 {
			if strings.Contains(stdErr, fmt.Sprintf("Remote branch %s not found", repo.Ref)) {
				return nil, "", fmt.Errorf(`reference "%s" not found in the templates git repository "%s"`, repo.Ref, repo.Url)
			}
			return nil, "", fmt.Errorf(`templates git repository not found on url "%s"`, repo.Url)
		}
		return nil, "", utils.PrefixError("cannot load template source directory", fmt.Errorf(stdErr))
	}

	// Create FS from the cloned repository
	localFs, err := aferofs.NewLocalFs(logger, dir, "")
	if err != nil {
		return nil, "", err
	}

	return localFs, dir, nil
}

func PullChangesToRepository(dir string, logger log.Logger) error {
	err, stdErr, _ := runGitCommand(logger, dir, []string{"pull", "origin"})
	if err != nil {
		return utils.PrefixError("cannot pull template source repository", fmt.Errorf(stdErr))
	}
	return nil
}

func CheckoutTemplateRepositoryPartial(ref model.TemplateRef, logger log.Logger) (filesystem.Fs, error) {
	if !Available() {
		return nil, fmt.Errorf("git command is not available, if you want to use templates from a git repository you have to install it first")
	}

	// Create a temp dir
	dir, err := ioutil.TempDir("", "keboola-as-code-templates-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err = os.RemoveAll(dir); err != nil { // nolint: forbidigo
			logger.Warnf(`cannot remove temp dir "%s": %w`, dir, err)
		}
	}()

	// Clone the repository
	err, stdErr, exitCode := runGitCommand(logger, dir, []string{"clone", "--branch", ref.Repository().Ref, "--depth=1", "--no-checkout", "--sparse", "--filter=blob:none", "-q", ref.Repository().Url, dir})
	if err != nil {
		if exitCode == 128 {
			if strings.Contains(stdErr, fmt.Sprintf("Remote branch %s not found", ref.Repository().Ref)) {
				return nil, fmt.Errorf(`reference "%s" not found in the templates git repository "%s"`, ref.Repository().Ref, ref.Repository().Url)
			}
			return nil, fmt.Errorf(`templates git repository not found on url "%s"`, ref.Repository().Url)
		}
		return nil, utils.PrefixError("cannot load template source directory", fmt.Errorf(stdErr))
	}

	// Checkout repository.json
	err, stdErr, _ = runGitCommand(logger, dir, []string{"sparse-checkout", "add", "/.keboola/repository.json"})
	if err != nil {
		return nil, fmt.Errorf(stdErr)
	}
	err, stdErr, _ = runGitCommand(logger, dir, []string{"checkout"})
	if err != nil {
		return nil, utils.PrefixError("cannot load template repository manifest", fmt.Errorf(stdErr))
	}

	// Create FS from the cloned repository
	localFs, err := aferofs.NewLocalFs(logger, dir, "")
	if err != nil {
		return nil, err
	}

	// Load the repository manifest
	m, err := manifest.Load(localFs)
	if err != nil {
		return nil, err
	}

	// Get version record
	version := ref.Version()
	versionRecord, err := m.GetVersion(ref.TemplateId(), version)
	if err != nil {
		// version or template not found
		e := utils.NewMultiError()
		e.Append(fmt.Errorf(`searched in git repository "%s"`, ref.Repository().Url))
		e.Append(fmt.Errorf(`reference "%s"`, ref.Repository().Ref))
		return nil, utils.PrefixError(err.Error(), e)
	}

	// Checkout template src directory
	srcDir := filesystem.Join(versionRecord.Path(), template.SrcDirectory)
	err, stdErr, _ = runGitCommand(logger, dir, []string{"sparse-checkout", "add", fmt.Sprintf("/%s", srcDir)})
	if err != nil {
		return nil, fmt.Errorf(stdErr)
	}
	if !localFs.Exists(srcDir) {
		e := utils.NewMultiError()
		e.Append(fmt.Errorf(`searched in git repository "%s"`, ref.Repository().Url))
		e.Append(fmt.Errorf(`reference "%s"`, ref.Repository().Ref))
		return nil, utils.PrefixError(fmt.Sprintf(`folder "%s" not found`, srcDir), e)
	}

	memFs, err := aferofs.NewMemoryFs(logger, ".")
	if err != nil {
		return nil, err
	}

	err = aferofs.CopyFs2Fs(localFs, "", memFs, "")
	if err != nil {
		return nil, err
	}

	return memFs, nil
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
