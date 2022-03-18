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

type Repository struct {
	model.TemplateRepository
	Fs     filesystem.Fs
	logger log.Logger
}

func (r *Repository) Pull() error {
	err, stdErr, _ := runGitCommand(r.logger, r.Fs.BasePath(), []string{"reset", "--hard", fmt.Sprintf("origin/%s", r.Ref)})
	if err != nil {
		return utils.PrefixError("cannot pull template source repository", fmt.Errorf(stdErr))
	}
	return nil
}

type CheckoutOptions struct {
	Partial            bool
	CopyToMemory       bool
	TemplateRepository model.TemplateRepository
	TemplateRef        model.TemplateRef
}

func CheckoutTemplateRepository(opts CheckoutOptions, logger log.Logger) (filesystem.Fs, error) {
	if !Available() {
		return nil, fmt.Errorf("git command is not available, if you want to use templates from a git repository you have to install it first")
	}

	// Create a temp dir
	dir, err := ioutil.TempDir("", "keboola-as-code-templates-")
	if err != nil {
		return nil, err
	}
	if opts.CopyToMemory {
		defer func() {
			if err = os.RemoveAll(dir); err != nil { // nolint: forbidigo
				logger.Warnf(`cannot remove temp dir "%s": %w`, dir, err)
			}
		}()
	}

	// Clone the repository
	cloneParams := []string{"clone", "--branch", opts.TemplateRepository.Ref}
	if opts.Partial {
		cloneParams = append(cloneParams, "--depth=1", "--no-checkout", "--sparse", "--filter=blob:none")
	}
	cloneParams = append(cloneParams, "-q", opts.TemplateRepository.Url, dir)
	err, stdErr, exitCode := runGitCommand(logger, dir, cloneParams)
	if err != nil {
		if exitCode == 128 {
			if strings.Contains(stdErr, fmt.Sprintf("Remote branch %s not found", opts.TemplateRepository.Ref)) {
				return nil, fmt.Errorf(`reference "%s" not found in the templates git repository "%s"`, opts.TemplateRepository.Ref, opts.TemplateRepository.Url)
			}
			return nil, fmt.Errorf(`templates git repository not found on url "%s"`, opts.TemplateRepository.Url)
		}
		return nil, utils.PrefixError("cannot load template source directory", fmt.Errorf(stdErr))
	}

	// Create FS from the cloned repository
	localFs, err := aferofs.NewLocalFs(logger, dir, "")
	if err != nil {
		return nil, err
	}

	if opts.Partial {
		// Checkout repository.json
		err, stdErr, _ = runGitCommand(logger, dir, []string{"sparse-checkout", "add", "/.keboola/repository.json"})
		if err != nil {
			return nil, fmt.Errorf(stdErr)
		}
		err, stdErr, _ = runGitCommand(logger, dir, []string{"checkout"})
		if err != nil {
			return nil, utils.PrefixError("cannot load template repository manifest", fmt.Errorf(stdErr))
		}

		versionRecord, err := getVersionFromRepositoryManifest(opts, localFs)
		if err != nil {
			return nil, err
		}

		// Checkout template src directory
		srcDir := filesystem.Join(versionRecord.Path(), template.SrcDirectory)
		err, stdErr, _ = runGitCommand(logger, dir, []string{"sparse-checkout", "add", fmt.Sprintf("/%s", srcDir)})
		if err != nil {
			return nil, fmt.Errorf(stdErr)
		}
		if !localFs.Exists(srcDir) {
			e := utils.NewMultiError()
			e.Append(fmt.Errorf(`searched in git repository "%s"`, opts.TemplateRepository.Url))
			e.Append(fmt.Errorf(`reference "%s"`, opts.TemplateRepository.Ref))
			return nil, utils.PrefixError(fmt.Sprintf(`folder "%s" not found`, srcDir), e)
		}
	}

	if opts.CopyToMemory {
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

	return localFs, nil
}

func getVersionFromRepositoryManifest(opts CheckoutOptions, localFs filesystem.Fs) (manifest.VersionRecord, error) {
	// Load the repository manifest
	m, err := manifest.Load(localFs)
	if err != nil {
		return manifest.VersionRecord{}, err
	}

	// Get version record
	version := opts.TemplateRef.Version()
	versionRecord, err := m.GetVersion(opts.TemplateRef.TemplateId(), version)
	if err != nil {
		// version or template not found
		e := utils.NewMultiError()
		e.Append(fmt.Errorf(`searched in git repository "%s"`, opts.TemplateRepository.Url))
		e.Append(fmt.Errorf(`reference "%s"`, opts.TemplateRepository.Ref))
		return manifest.VersionRecord{}, utils.PrefixError(err.Error(), e)
	}
	return versionRecord, nil
}

func CheckoutTemplateRepositoryFull(repo model.TemplateRepository, logger log.Logger) (*Repository, error) {
	localFs, err := CheckoutTemplateRepository(CheckoutOptions{
		Partial:            false,
		CopyToMemory:       false,
		TemplateRepository: repo,
		TemplateRef:        nil,
	}, logger)
	if err != nil {
		return nil, err
	}

	return &Repository{
		TemplateRepository: repo,
		Fs:                 localFs,
	}, nil
}

func CheckoutTemplateRepositoryPartial(ref model.TemplateRef, logger log.Logger) (filesystem.Fs, error) {
	return CheckoutTemplateRepository(CheckoutOptions{
		Partial:            true,
		CopyToMemory:       true,
		TemplateRepository: ref.Repository(),
		TemplateRef:        ref,
	}, logger)
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
