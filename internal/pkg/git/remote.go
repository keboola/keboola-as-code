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
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type RemoteRepository struct {
	ref         model.TemplateRepository
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

type PullResult struct {
	OldHash string
	NewHash string
	Changed bool
}

type cmdResult struct {
	exitCode int
	stdOut   string
	stdErr   string
}

func Available() bool {
	_, err := exec.LookPath("git")
	return err == nil
}

func Checkout(ctx context.Context, ref model.TemplateRepository, sparse bool, logger log.Logger) (r *RemoteRepository, err error) {
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
	r = &RemoteRepository{
		ref:         ref,
		sparse:      sparse,
		logger:      logger,
		baseDirPath: baseDirPath,
		workingDir:  workingDirFs,
		valuesLock:  &sync.RWMutex{},
		cmdLock:     &sync.Mutex{},
	}

	// Clone parameters
	params := []string{"clone", "-q", "--depth=1", "--branch", r.Ref()}
	if r.sparse {
		params = append(params, "--no-checkout", "--sparse", "--filter=blob:none")
	}
	params = append(params, "--", r.Url(), workingDir)

	// Clone repository
	result, err := r.runGitCmd(ctx, params...)
	if err != nil {
		if strings.Contains(result.stdErr, fmt.Sprintf("Remote branch %s not found", r.Ref())) {
			return nil, fmt.Errorf(`reference "%s" not found in the git repository "%s"`, r.Ref(), r.Url())
		}
		out := errorMsg(result, err)
		return nil, fmt.Errorf(`git repository could not be checked out from "%s": %s`, r.Url(), out)
	}

	// Setup stable dir
	if err := r.copyWorkingToStableDir(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *RemoteRepository) String() string {
	return fmt.Sprintf("%s:%s", r.Url(), r.Ref())
}

func (r *RemoteRepository) Definition() model.TemplateRepository {
	return r.ref
}

// Url to Git repository.
func (r *RemoteRepository) Url() string {
	return r.ref.Url
}

// Ref is Git branch or tag.
func (r *RemoteRepository) Ref() string {
	return r.ref.Ref
}

func (r *RemoteRepository) WorkingFs() filesystem.Fs {
	return r.workingDir
}

// Fs return repository filesystem.
func (r *RemoteRepository) Fs() (filesystem.Fs, RepositoryFsUnlockFn) {
	// Sync access to the "stableDir" field
	r.valuesLock.RLock()
	defer r.valuesLock.RUnlock()

	// Postpone free operation when FS is used
	r.stableDir.freeLock.RLock()
	return r.stableDir._fs, r.stableDir.freeLock.RUnlock
}

func (r *RemoteRepository) CommitHash() string {
	r.valuesLock.RLock()
	defer r.valuesLock.RUnlock()
	return r.stableCommitHash
}

func (r *RemoteRepository) obtainCommitHash(ctx context.Context) (string, error) {
	result, err := r.runGitCmd(ctx, "rev-parse", "HEAD")
	if err != nil {
		out := errorMsg(result, err)
		return "", fmt.Errorf(`cannot get repository hash: %s`, out)
	}
	return strings.TrimSuffix(result.stdOut, "\n"), nil
}

// Load a path from the remote git repository, if sparse mode is used.
func (r *RemoteRepository) Load(ctx context.Context, path string) error {
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

func (r *RemoteRepository) Pull(ctx context.Context) (*PullResult, error) {
	// Check remote changes
	if err := r.fetch(ctx); err != nil {
		return nil, err
	}

	// Reset is used, because it works also with force push (edge-case)
	cmdResult, err := r.runGitCmd(ctx, "reset", "--hard", fmt.Sprintf("origin/%s", r.Ref()))
	if err != nil {
		out := errorMsg(cmdResult, err)
		return nil, fmt.Errorf(`cannot reset repository to the origin: %s`, out)
	}

	// Get new commit hash
	commitHash, err := r.obtainCommitHash(ctx)
	if err != nil {
		return nil, err
	}

	// Update stable dir if commit hash differs
	pullResult := &PullResult{OldHash: r.stableCommitHash, NewHash: commitHash, Changed: commitHash != r.stableCommitHash}
	if pullResult.Changed {
		if err := r.copyWorkingToStableDir(ctx); err != nil {
			return nil, err
		}
	}

	return pullResult, nil
}

func (r *RemoteRepository) Free() <-chan struct{} {
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

func (r *RemoteRepository) fetch(ctx context.Context) error {
	// Check remote changes
	result, err := r.runGitCmd(ctx, "fetch", "origin")
	if err != nil {
		out := errorMsg(result, err)
		return fmt.Errorf(`cannot fetch repository: %s`, out)
	}

	return nil
}

func (r *RemoteRepository) runGitCmd(ctx context.Context, args ...string) (cmdResult, error) {
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

func (r *RemoteRepository) doRunGitCmd(ctx context.Context, args ...string) (cmdResult, error) {
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

func (r *RemoteRepository) copyWorkingToStableDir(ctx context.Context) (err error) {
	// Create stable dir
	commitHash, err := r.obtainCommitHash(ctx)
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
