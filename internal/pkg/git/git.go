package git

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type Repository struct {
	url       string
	ref       string
	sparse    bool
	logger    log.Logger
	lock      *sync.RWMutex
	fetchLock *sync.Mutex
	fs        filesystem.Fs
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

func Checkout(ctx context.Context, url, ref string, sparse bool, logger log.Logger) (*Repository, error) {
	if !Available() {
		return nil, fmt.Errorf("git command is not available, you have to install it first")
	}

	// Create a temp dir, it must be later cleared by calling Repository.Clear()
	dir, err := ioutil.TempDir("", "keboola-as-code-git-")
	if err != nil {
		return nil, err
	}
	fs, err := aferofs.NewLocalFs(logger, dir, "")
	if err != nil {
		return nil, err
	}

	// Create repository
	r := &Repository{url: url, ref: ref, sparse: sparse, logger: logger, lock: &sync.RWMutex{}, fetchLock: &sync.Mutex{}, fs: fs}

	// Clone parameters
	params := []string{"clone", "-q", "--branch", r.ref}
	if r.sparse {
		params = append(params, "--depth=1", "--no-checkout", "--sparse", "--filter=blob:none")
	}
	params = append(params, "--", r.url, dir)

	// Clone repository
	result, err := r.runGitCmd(ctx, params...)
	if err != nil {
		if strings.Contains(result.stdErr, fmt.Sprintf("Remote branch %s not found", r.ref)) {
			return nil, fmt.Errorf(`reference "%s" not found in the git repository "%s"`, r.ref, r.url)
		}
		out := errorMsg(result, err)
		return nil, fmt.Errorf(`git repository could not be checked out from "%s": %s`, r.url, out)
	}
	return r, nil
}

func errorMsg(result cmdResult, err error) string {
	stderr := strings.TrimSpace(result.stdErr)
	if stderr != "" {
		return stderr
	}
	stdout := strings.TrimSpace(result.stdOut)
	if stdout != "" {
		return stdout
	}
	return err.Error()
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

// RLock acquire read lock, before reading from repository Fs.
// Pull() is blocked until the read is finished.
func (r *Repository) RLock() {
	r.lock.RLock()
}

// RUnlock release read lock.
func (r *Repository) RUnlock() {
	r.lock.RUnlock()
}

// Fs return repository filesystem.
// It must be used together with RLock and RUnlock method, to sync Pull() with Fs() reads.
func (r *Repository) Fs() filesystem.Fs {
	return r.fs
}

// Clear deletes temp directory with all files.
func (r *Repository) Clear() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if err := os.RemoveAll(r.fs.BasePath()); err != nil { // nolint: forbidigo
		r.logger.Warnf(`cannot remove temp dir "%s": %w`, r.fs.BasePath(), err)
	}
	r.fs = nil
}

func (r *Repository) CommitHash(ctx context.Context) (string, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
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
	if _, err := r.runGitCmd(ctx, "sparse-checkout", "add", fmt.Sprintf("/%s", path)); err != nil {
		return err
	}
	if _, err := r.runGitCmd(ctx, "checkout"); err != nil {
		return err
	}
	return nil
}

func (r *Repository) Pull(ctx context.Context) error {
	// Check remote changes
	if err := r.fetch(ctx); err != nil {
		return err
	}

	// Acquire write lock, from the repository must not be read during this time
	r.lock.Lock()
	defer r.lock.Unlock()

	// Reset is used, because it works also with force push (edge-case)
	result, err := r.runGitCmd(ctx, "reset", "--hard", fmt.Sprintf("origin/%s", r.ref))
	if err != nil {
		out := errorMsg(result, err)
		return fmt.Errorf(`cannot reset repository to the origin: %s`, out)
	}

	return nil
}

func (r *Repository) fetch(ctx context.Context) error {
	r.fetchLock.Lock()
	defer r.fetchLock.Unlock()

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
	r.logger.Debug(fmt.Sprintf(`Running git command: git %s`, strings.Join(args, " ")))

	var stdOutBuffer bytes.Buffer
	var stdErrBuffer bytes.Buffer

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = r.fs.BasePath()
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

func newBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 50 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 500 * time.Millisecond
	b.MaxElapsedTime = 2 * time.Second
	b.Reset()
	return b
}
