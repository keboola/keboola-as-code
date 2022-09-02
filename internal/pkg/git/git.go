// nolint: forbidigo
package git

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type Repository struct {
	url         string
	ref         string
	sparse      bool
	logger      log.Logger
	baseDirPath string        // base directory for working and stable dir
	workingDir  filesystem.Fs // directory for work and updates

	valuesLock       *sync.RWMutex // sync access to stableDir and stableCommitHash fields
	stableDir        *fsWithFreeLock
	stableCommitHash string

	cmdLock *sync.Mutex // only one git command can run at a time
}

type RepositoryFsUnlockFn func()

type cmdResult struct {
	exitCode int
	stdOut   string
	stdErr   string
}

func Available() bool {
	_, err := exec.LookPath("git")
	return err == nil
}

func Checkout(ctx context.Context, url, ref string, sparse bool, logger log.Logger) (r *Repository, err error) {
	if !Available() {
		return nil, fmt.Errorf("git command is not available, you have to install it first")
	}

	// Create base directory
	baseDirPath, err := os.MkdirTemp("", "kac-git-repository-")
	if err != nil {
		return nil, fmt.Errorf("cannot create temp dir for git repository: %w", err)
	}

	// Clear everything if checkout fails
	defer func() {
		if err != nil {
			_ = os.RemoveAll(baseDirPath)
		}
	}()

	// Create working directory
	workingDir := filepath.Join(baseDirPath, "working")
	if err := os.Mkdir(workingDir, 0700); err != nil {
		return nil, fmt.Errorf("cannot create working dir for git repository: %w", err)
	}
	workingDirFs, err := aferofs.NewLocalFs(logger, workingDir, "")
	if err != nil {
		return nil, fmt.Errorf("cannot setup working fs for git repository: %w", err)
	}

	// Create repository
	r = &Repository{
		url:         url,
		ref:         ref,
		sparse:      sparse,
		logger:      logger,
		baseDirPath: baseDirPath,
		workingDir:  workingDirFs,
		valuesLock:  &sync.RWMutex{},
		cmdLock:     &sync.Mutex{},
	}

	// Clone parameters
	params := []string{"clone", "-q", "--branch", r.ref}
	if r.sparse {
		params = append(params, "--depth=1", "--no-checkout", "--sparse", "--filter=blob:none")
	}
	params = append(params, "--", r.url, workingDir)

	// Clone repository
	result, err := r.runGitCmd(ctx, params...)
	if err != nil {
		if strings.Contains(result.stdErr, fmt.Sprintf("Remote branch %s not found", r.ref)) {
			return nil, fmt.Errorf(`reference "%s" not found in the git repository "%s"`, r.ref, r.url)
		}
		out := errorMsg(result, err)
		return nil, fmt.Errorf(`git repository could not be checked out from "%s": %s`, r.url, out)
	}

	// Setup stable dir
	if err := r.copyWorkingToStableDir(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Repository) String() string {
	return fmt.Sprintf("%s:%s", r.url, r.ref)
}

// Url to Git repository.
func (r *Repository) Url() string {
	return r.url
}

// Ref is Git branch or tag.
func (r *Repository) Ref() string {
	return r.ref
}

func (r *Repository) WorkingFs() filesystem.Fs {
	return r.workingDir
}

// Fs return repository filesystem.
func (r *Repository) Fs() (filesystem.Fs, RepositoryFsUnlockFn) {
	// Sync access to the "stableDir" field
	r.valuesLock.RLock()
	defer r.valuesLock.RUnlock()

	// Postpone free operation when FS is used
	r.stableDir.freeLock.RLock()
	return r.stableDir._fs, r.stableDir.freeLock.RUnlock
}

func (r *Repository) CommitHash(ctx context.Context) (string, error) {
	result, err := r.runGitCmd(ctx, "rev-parse", "HEAD")
	if err != nil {
		out := errorMsg(result, err)
		return "", fmt.Errorf(`cannot get repository hash: %s`, out)
	}
	return strings.TrimSuffix(result.stdOut, "\n"), nil
}

// Load a path from the remote git repository, if sparse mode is used.
func (r *Repository) Load(ctx context.Context, path string) error {
	if !r.sparse {
		return fmt.Errorf("sparse checkout is not allowed")
	}
	if _, err := r.runGitCmd(ctx, "sparse-checkout", "add", path); err != nil {
		return err
	}
	if _, err := r.runGitCmd(ctx, "checkout"); err != nil {
		return err
	}

	if err := r.copyWorkingToStableDir(ctx); err != nil {
		return err
	}

	return nil
}

func (r *Repository) Pull(ctx context.Context) error {
	// Check remote changes
	if err := r.fetch(ctx); err != nil {
		return err
	}

	// Reset is used, because it works also with force push (edge-case)
	result, err := r.runGitCmd(ctx, "reset", "--hard", fmt.Sprintf("origin/%s", r.ref))
	if err != nil {
		out := errorMsg(result, err)
		return fmt.Errorf(`cannot reset repository to the origin: %s`, out)
	}

	// Get new commit hash
	commitHash, err := r.CommitHash(ctx)
	if err != nil {
		return err
	}

	// Update stable dir if commit hash differs
	if commitHash != r.stableCommitHash {
		if err := r.copyWorkingToStableDir(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) Free() <-chan struct{} {
	done := make(chan struct{})

	go func() {
		// Sync access to the "stableDir" field
		r.valuesLock.Lock()
		defer r.valuesLock.Unlock()

		<-r.stableDir.free()
		r.stableDir = nil

		_ = os.RemoveAll(r.baseDirPath)
		close(done)
	}()

	return done
}

func (r *Repository) fetch(ctx context.Context) error {
	// Check remote changes
	result, err := r.runGitCmd(ctx, "fetch", "origin")
	if err != nil {
		out := errorMsg(result, err)
		return fmt.Errorf(`cannot fetch repository: %s`, out)
	}

	return nil
}

func (r *Repository) runGitCmd(ctx context.Context, args ...string) (cmdResult, error) {
	retry := newBackoff()
	for {
		result, err := r.doRunGitCmd(ctx, args...)
		if result.exitCode == 0 && err == nil {
			return result, err
		}
		if delay := retry.NextBackOff(); delay == retry.Stop {
			return result, err
		} else {
			time.Sleep(delay)
		}
	}
}

func (r *Repository) doRunGitCmd(ctx context.Context, args ...string) (cmdResult, error) {
	r.cmdLock.Lock()
	defer r.cmdLock.Unlock()
	r.logger.Debug(fmt.Sprintf(`Running git command: git %s`, strings.Join(args, " ")))

	var stdOutBuffer bytes.Buffer
	var stdErrBuffer bytes.Buffer

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = r.workingDir.BasePath()
	cmd.Stdout = io.MultiWriter(r.logger.DebugWriter(), &stdOutBuffer)
	cmd.Stderr = io.MultiWriter(r.logger.DebugWriter(), &stdErrBuffer)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0")

	err := cmd.Run()
	result := cmdResult{}
	result.stdOut = stdOutBuffer.String()
	result.stdErr = stdErrBuffer.String()
	result.exitCode = 0
	if err != nil {
		// nolint: errorlint
		if exitError, ok := err.(*exec.ExitError); ok {
			result.exitCode = exitError.ExitCode()
		}
	}

	return result, err
}

func (r *Repository) copyWorkingToStableDir(ctx context.Context) (err error) {
	// Create stable dir
	commitHash, err := r.CommitHash(ctx)
	if err != nil {
		return err
	}
	stableDir := filepath.Join(r.baseDirPath, commitHash+"-"+gonanoid.Must(8))
	if err := os.Mkdir(stableDir, 0700); err != nil {
		return fmt.Errorf("cannot create stable dir for git repository: %w", err)
	}

	// Clear stable dir if checkout fails
	defer func() {
		if err != nil {
			_ = os.RemoveAll(stableDir)
		}
	}()

	// Create FS
	stableDirFs, err := aferofs.NewLocalFs(r.logger, stableDir, "")
	if err != nil {
		return fmt.Errorf("cannot setup stable fs for git repository: %w", err)
	}

	// Copy working dir to stable dir
	if err := aferofs.CopyFs2Fs(r.workingDir, "", stableDirFs, ""); err != nil {
		return fmt.Errorf("cannot copy working dir to stable dir: %w", err)
	}

	// Sync access to fields
	r.valuesLock.Lock()
	defer r.valuesLock.Unlock()

	// Free old stable dir
	if r.stableDir != nil {
		r.stableDir.free()
	}

	// Replace values
	r.stableDir = newFsWithFreeLock(stableDirFs)
	r.stableCommitHash = commitHash

	return nil
}

type _fs = filesystem.Fs

type fsWithFreeLock struct {
	_fs
	freeLock *sync.RWMutex
}

func newFsWithFreeLock(fs filesystem.Fs) *fsWithFreeLock {
	return &fsWithFreeLock{_fs: fs, freeLock: &sync.RWMutex{}}
}

func (v *fsWithFreeLock) free() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		lock := v.freeLock

		// Postpone free operation until the FS is no longer in use
		lock.Lock()
		defer lock.Unlock()

		// Delete directory
		_ = os.RemoveAll(v._fs.BasePath())

		// Clear values
		v._fs = nil
		v.freeLock = nil
		close(done)
	}()
	return done
}

func newBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 200 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 500 * time.Millisecond
	b.MaxElapsedTime = 2 * time.Second
	b.Reset()
	return b
}

func errorMsg(result cmdResult, err error) string {
	return fmt.Sprintf("%s\n\nstderr:\n%s\n\nstdout:\n%s", err.Error(), strings.TrimSpace(result.stdErr), strings.TrimSpace(result.stdOut))
}
