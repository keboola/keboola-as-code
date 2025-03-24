package

// nolint: forbidigo
git

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	timebackoff "github.com/keboola/keboola-as-code/internal/pkg/utils/backoff"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type RemoteRepository struct {
	ref         model.TemplateRepository
	sparse      bool
	logger      log.Logger
	baseDirPath string        // base directory for working and stable dir
	workingDir  filesystem.Fs // directory for work and updates

	valuesLock       *deadlock.RWMutex // sync access to stableDir and stableCommitHash fields
	stableDir        *fsWithFreeLock
	stableCommitHash string

	cmdLock *deadlock.Mutex // only one git command can run at a time
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
		return nil, errors.New("git command is not available, you have to install it first")
	}

	// Create base directory
	baseDirPath, err := os.MkdirTemp("", "kac-git-repository-")
	if err != nil {
		return nil, errors.Errorf("cannot create temp dir for git repository: %w", err)
	}

	// Clear everything if checkout fails
	defer func() {
		if err != nil {
			_ = os.RemoveAll(baseDirPath)
		}
	}()

	// Create working directory
	workingDir := filepath.Join(baseDirPath, "working")
	if err := os.Mkdir(workingDir, 0o700); err != nil {
		return nil, errors.Errorf("cannot create working dir for git repository: %w", err)
	}
	workingDirFs, err := aferofs.NewLocalFs(workingDir, filesystem.WithLogger(logger))
	if err != nil {
		return nil, errors.Errorf("cannot setup working fs for git repository: %w", err)
	}

	// Create repository
	r = &RemoteRepository{
		ref:         ref,
		sparse:      sparse,
		logger:      logger,
		baseDirPath: baseDirPath,
		workingDir:  workingDirFs,
		valuesLock:  &deadlock.RWMutex{},
		cmdLock:     &deadlock.Mutex{},
	}

	// Clone parameters
	params := []string{"clone", "-q", "--depth=1", "--branch", r.Ref()}
	if r.sparse {
		params = append(params, "--no-checkout", "--sparse", "--filter=blob:none")
	}
	params = append(params, "--", r.URL(), workingDir)

	// Clone repository
	result, err := r.runGitCmd(ctx, params...)
	if err != nil {
		if strings.Contains(result.stdErr, fmt.Sprintf("Remote branch %s not found", r.Ref())) {
			return nil, errors.Errorf(`reference "%s" not found in the git repository "%s"`, r.Ref(), r.URL())
		}
		out := errorMsg(result, err)
		return nil, errors.Errorf(`git repository could not be checked out from "%s": %s`, r.URL(), out)
	}

	// Setup stable dir
	if err := r.copyWorkingToStableDir(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *RemoteRepository) String() string {
	return fmt.Sprintf("%s:%s", r.URL(), r.Ref())
}

func (r *RemoteRepository) Definition() model.TemplateRepository {
	return r.ref
}

// URL to Git repository.
func (r *RemoteRepository) URL() string {
	return r.ref.URL
}

// Ref is a Git branch or tag.
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
		return "", errors.Errorf(`cannot get repository hash: %s`, out)
	}
	return strings.TrimSuffix(result.stdOut, "\n"), nil
}

// Load a path from the remote git repository, if sparse mode is used.
func (r *RemoteRepository) Load(ctx context.Context, path string) error {
	if !r.sparse {
		return errors.New("sparse checkout is not allowed")
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
		return nil, errors.Errorf(`cannot reset repository to the origin: %s`, out)
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
		return errors.Errorf(`cannot fetch repository: %s`, out)
	}

	return nil
}

func (r *RemoteRepository) runGitCmd(ctx context.Context, args ...string) (cmdResult, error) {
	var lastErr error
	retry := newBackoff()
	for {
		result, err := r.doRunGitCmd(ctx, args...)
		if result.exitCode == 0 && err == nil {
			// Success
			return result, nil
		}
		if lastErr == nil || (!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
			// Set the last error, if it is not context cancelled or deadline exceeded.
			// We want to return the reason why retries were made.
			lastErr = err
		}
		if delay := retry.NextBackOff(); delay == backoff.Stop {
			return result, lastErr
		} else {
			select {
			case <-ctx.Done():
				return result, lastErr
			case <-time.After(delay):
				// continue, try again
			}
		}
	}
}

func (r *RemoteRepository) doRunGitCmd(ctx context.Context, args ...string) (cmdResult, error) {
	r.cmdLock.Lock()
	defer r.cmdLock.Unlock()
	r.logger.Debugf(ctx, `Running git command: git %s`, strings.Join(args, " "))

	var stdOutBuffer bytes.Buffer
	var stdErrBuffer bytes.Buffer

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = r.workingDir.BasePath()
	cmd.Stdout = &stdOutBuffer
	cmd.Stderr = &stdErrBuffer
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
	stableDir := filepath.Join(r.baseDirPath, commitHash+"-"+idgenerator.Random(8))
	if err := os.Mkdir(stableDir, 0o700); err != nil {
		return errors.Errorf("cannot create stable dir for git repository: %w", err)
	}

	// Clear stable dir if checkout fails
	defer func() {
		if err != nil {
			_ = os.RemoveAll(stableDir)
		}
	}()

	// Create FS
	stableDirFs, err := aferofs.NewLocalFs(stableDir, filesystem.WithLogger(r.logger))
	if err != nil {
		return errors.Errorf("cannot setup stable fs for git repository: %w", err)
	}

	// Copy working dir to stable dir
	if err := aferofs.CopyFs2Fs(r.workingDir, "", stableDirFs, ""); err != nil {
		return errors.Errorf("cannot copy working dir to stable dir: %w", err)
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
	freeLock *deadlock.RWMutex
}

func newFsWithFreeLock(fs filesystem.Fs) *fsWithFreeLock {
	return &fsWithFreeLock{_fs: fs, freeLock: &deadlock.RWMutex{}}
}

func (v *fsWithFreeLock) free() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		lock := v.freeLock

		// Postpone free operation until the FS is no longer in use
		lock.Lock()
		defer lock.Unlock()

		// Delete directory
		_ = os.RemoveAll(v.BasePath())

		// Clear values
		v._fs = nil
		v.freeLock = nil
		close(done)
	}()
	return done
}

func newBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 200 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 500 * time.Millisecond
	tb := timebackoff.NewTimeBackoff(b, 2*time.Second)
	tb.Reset()
	return tb
}

func errorMsg(result cmdResult, err error) string {
	return fmt.Sprintf("%s\n\nstderr:\n%s\n\nstdout:\n%s", err.Error(), strings.TrimSpace(result.stdErr), strings.TrimSpace(result.stdOut))
}
