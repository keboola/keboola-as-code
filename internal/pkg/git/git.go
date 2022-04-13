package git

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"

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

type repoRef = model.TemplateRepository

type Repository struct {
	repoRef
	fs     filesystem.Fs
	logger log.Logger
	lock   *sync.RWMutex
}

func (r *Repository) CommitHash() (string, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	err, stdErr, _, stdOut := runGitCommand(r.logger, r.fs.BasePath(), []string{"rev-parse", "HEAD"})
	if err != nil {
		return "", utils.PrefixError("cannot get repository hash", fmt.Errorf(stdErr))
	}
	return strings.TrimSuffix(stdOut, "\n"), nil
}

func (r *Repository) RLock() {
	r.lock.RLock()
}

func (r *Repository) RUnlock() {
	r.lock.RUnlock()
}

func (r *Repository) Fs() filesystem.Fs {
	return r.fs
}

func (r *Repository) Pull() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	err, stdErr, _, _ := runGitCommand(r.logger, r.fs.BasePath(), []string{"fetch", "origin"})
	if err != nil {
		return utils.PrefixError("cannot fetch template repository", fmt.Errorf(stdErr))
	}

	err, stdErr, _, _ = runGitCommand(r.logger, r.fs.BasePath(), []string{"reset", "--hard", fmt.Sprintf("origin/%s", r.Ref)})
	if err != nil {
		return utils.PrefixError("cannot reset template repository to the origin", fmt.Errorf(stdErr))
	}

	return nil
}

type CheckoutOptions struct {
	CloneParams        []string // Params for git clone
	Partial            bool     // Partially checkout just the selected template repository (and the repository manifest)
	ToMemory           bool     // Move the FS with the repository to memory
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
	if opts.ToMemory {
		defer func() {
			if err = os.RemoveAll(dir); err != nil { // nolint: forbidigo
				logger.Warnf(`cannot remove temp dir "%s": %w`, dir, err)
			}
		}()
	}

	// Clone the repository
	cloneParams := append([]string{"clone"}, opts.CloneParams...)
	cloneParams = append(cloneParams, dir)
	err, stdErr, exitCode, _ := runGitCommand(logger, dir, cloneParams)
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
		err, stdErr, _, _ = runGitCommand(logger, dir, []string{"sparse-checkout", "add", "/.keboola/repository.json"})
		if err != nil {
			return nil, fmt.Errorf(stdErr)
		}
		err, stdErr, _, _ = runGitCommand(logger, dir, []string{"checkout"})
		if err != nil {
			return nil, utils.PrefixError("cannot load template repository manifest", fmt.Errorf(stdErr))
		}

		versionRecord, err := getVersionFromRepositoryManifest(opts, localFs)
		if err != nil {
			return nil, err
		}

		// Checkout template src directory
		srcDir := filesystem.Join(versionRecord.Path(), template.SrcDirectory)
		err, stdErr, _, _ = runGitCommand(logger, dir, []string{"sparse-checkout", "add", fmt.Sprintf("/%s", srcDir)})
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

	if opts.ToMemory {
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

func CheckoutTemplateRepositoryFull(templateRepo model.TemplateRepository, logger log.Logger) (*Repository, error) {
	localFs, err := CheckoutTemplateRepository(CheckoutOptions{
		Partial:            false,
		ToMemory:           false,
		TemplateRepository: templateRepo,
		TemplateRef:        nil,
		CloneParams:        []string{"--branch", templateRepo.Ref, "-q", templateRepo.Url},
	}, logger)
	if err != nil {
		return nil, err
	}

	repo := &Repository{repoRef: templateRepo, fs: localFs, logger: logger, lock: &sync.RWMutex{}}
	hash, err := repo.CommitHash()
	if err != nil {
		logger.Warnf(`repository "%s:%s" checked out but the commit hash was not retrieved due to: %w`, repo.Url, repo.Ref, err)
		return repo, nil
	}

	logger.Infof(`repository "%s:%s" checked out to %s`, repo.Url, repo.Ref, hash)
	return repo, nil
}

func CheckoutTemplateRepositoryPartial(ref model.TemplateRef, logger log.Logger) (filesystem.Fs, error) {
	return CheckoutTemplateRepository(CheckoutOptions{
		Partial:            true,
		ToMemory:           true,
		TemplateRepository: ref.Repository(),
		TemplateRef:        ref,
		CloneParams:        []string{"--branch", ref.Repository().Ref, "--depth=1", "--no-checkout", "--sparse", "--filter=blob:none", "-q", ref.Repository().Url},
	}, logger)
}

func runGitCommand(logger log.Logger, dir string, args []string) (err error, stdErr string, exitCode int, stdOut string) {
	logger.Debug(fmt.Sprintf(`Running git command: git %s`, strings.Join(args, " ")))
	var stdOutBuffer bytes.Buffer
	var stdErrBuffer bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = io.MultiWriter(logger.DebugWriter(), &stdOutBuffer)
	cmd.Stderr = io.MultiWriter(logger.DebugWriter(), &stdErrBuffer)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0")
	err = cmd.Run()
	stdOut = stdOutBuffer.String()
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
