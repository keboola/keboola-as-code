// Package gitbranch provides utilities for git branch operations in the CLI project context.
package gitbranch

import (
	"context"
	"os/exec"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CurrentBranch returns the current git branch name.
func CurrentBranch(ctx context.Context, fs filesystem.Fs) (string, error) {
	return currentBranchInDir(ctx, fs.BasePath())
}

// currentBranchInDir returns the current git branch in the specified directory.
func currentBranchInDir(ctx context.Context, dir string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "branch", "--show-current")
	cmd.Dir = dir
	output, err := cmd.Output()
	if err != nil {
		return "", errors.Errorf("cannot get current git branch: %w", err)
	}
	branch := strings.TrimSpace(string(output))
	if branch == "" {
		return "", errors.New("cannot determine current git branch (detached HEAD?)")
	}
	return branch, nil
}

// IsGitRepository checks if the directory is a git repository.
func IsGitRepository(ctx context.Context, fs filesystem.Fs) bool {
	return fs.IsDir(ctx, ".git")
}

// DefaultBranchFromRemote tries to detect the default branch from the git remote.
func DefaultBranchFromRemote(ctx context.Context, fs filesystem.Fs) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "symbolic-ref", "refs/remotes/origin/HEAD")
	cmd.Dir = fs.BasePath()
	output, err := cmd.Output()
	if err != nil {
		return "", errors.Errorf("cannot detect default branch from remote: %w", err)
	}
	// Parse "refs/remotes/origin/main" -> "main"
	ref := strings.TrimSpace(string(output))
	parts := strings.Split(ref, "/")
	if len(parts) < 4 {
		return "", errors.Errorf("unexpected remote HEAD format: %s", ref)
	}
	return parts[len(parts)-1], nil
}

// HasRemote checks if the git repository has a remote configured.
func HasRemote(ctx context.Context, fs filesystem.Fs) bool {
	cmd := exec.CommandContext(ctx, "git", "remote")
	cmd.Dir = fs.BasePath()
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) != ""
}

// IsDefaultBranch checks if the given branch name is a common default branch name.
func IsDefaultBranch(name string) bool {
	return name == "main" || name == "master"
}
