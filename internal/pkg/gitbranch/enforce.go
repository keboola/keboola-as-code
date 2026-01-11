package gitbranch

import (
	"context"
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/branchmapping"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const BranchIDOverrideENV = "KBC_BRANCH_ID"

// EnforceResult contains the result of branch enforcement check.
type EnforceResult struct {
	GitBranch   string
	BranchID    string // empty string for production
	BranchName  string
	EnvOverride map[string]string // environment variables to set
}

// EnforceBranchMapping checks if git-branching mode is enabled and if so,
// verifies that the current git branch is linked to a Keboola branch.
// Returns nil result if git-branching is not enabled.
func EnforceBranchMapping(
	ctx context.Context,
	fs filesystem.Fs,
	manifest *projectManifest.Manifest,
	envs env.Provider,
	logger log.Logger,
) (*EnforceResult, error) {
	// Check if git-branching is enabled
	if !manifest.IsGitBranchingEnabled() {
		return nil, nil
	}

	// Check if we're in a git repository
	if !IsGitRepository(ctx, fs) {
		return nil, errors.New("git repository not found but git-branching mode is enabled")
	}

	// Get current git branch
	gitBranch, err := CurrentBranch(ctx, fs)
	if err != nil {
		return nil, err
	}

	// Check if branch mapping file exists
	if !branchmapping.Exists(ctx, fs) {
		return nil, errors.Errorf("git branch not linked to Keboola branch\n\n   Current git branch: %s\n\n   This project uses git-branching mode, which requires each git branch\n   to be explicitly linked to a Keboola branch before sync operations.\n\n   To fix:\n   kbc branch link              # Create and link a new Keboola branch\n   kbc branch link --branch-id=<id>  # Link to existing branch", gitBranch)
	}

	// Load branch mappings
	mappings, err := branchmapping.Load(ctx, fs)
	if err != nil {
		return nil, errors.Errorf("failed to load branch mappings: %w", err)
	}

	// Check if current branch is mapped
	mapping, found := mappings.GetMapping(gitBranch)
	if !found {
		return nil, errors.Errorf("git branch not linked to Keboola branch\n\n   Current git branch: %s\n\n   This project uses git-branching mode, which requires each git branch\n   to be explicitly linked to a Keboola branch before sync operations.\n\n   To fix:\n   kbc branch link              # Create and link a new Keboola branch\n   kbc branch link --branch-id=<id>  # Link to existing branch", gitBranch)
	}

	result := &EnforceResult{
		GitBranch:   gitBranch,
		BranchName:  mapping.Name,
		EnvOverride: make(map[string]string),
	}

	// Get the branch ID
	if mapping.IsProduction() {
		// Production branch - no override needed, but log for clarity
		logger.Infof(ctx, "Using Keboola branch: %s (production)", mapping.Name)
	} else {
		result.BranchID = mapping.GetID()
		logger.Infof(ctx, "Using Keboola branch: %s (ID: %s)", mapping.Name, result.BranchID)

		// Check if KBC_BRANCH_ID is already set
		existingBranchID := envs.Get(BranchIDOverrideENV)
		if existingBranchID != "" && existingBranchID != result.BranchID {
			logger.Warnf(ctx, "Overriding %s=%s with git-branching value %s", BranchIDOverrideENV, existingBranchID, result.BranchID)
		}

		// Set environment override
		result.EnvOverride[BranchIDOverrideENV] = result.BranchID
	}

	return result, nil
}

// SetBranchIDEnv sets the KBC_BRANCH_ID environment variable in the given env.Map.
func SetBranchIDEnv(envs *env.Map, branchID string) {
	if branchID != "" {
		envs.Set(BranchIDOverrideENV, branchID)
	}
}

// GetBranchIDFromMapping returns the branch ID for the current git branch.
// Returns 0 for production branch or if git-branching is not enabled.
func GetBranchIDFromMapping(ctx context.Context, fs filesystem.Fs, manifest *projectManifest.Manifest) (int, error) {
	if !manifest.IsGitBranchingEnabled() {
		return 0, nil
	}

	if !IsGitRepository(ctx, fs) {
		return 0, nil
	}

	gitBranch, err := CurrentBranch(ctx, fs)
	if err != nil {
		return 0, err
	}

	if !branchmapping.Exists(ctx, fs) {
		return 0, nil
	}

	mappings, err := branchmapping.Load(ctx, fs)
	if err != nil {
		return 0, err
	}

	mapping, found := mappings.GetMapping(gitBranch)
	if !found || mapping.IsProduction() {
		return 0, nil
	}

	branchID, err := strconv.Atoi(mapping.GetID())
	if err != nil {
		return 0, errors.Errorf("invalid branch ID in mapping: %s", mapping.GetID())
	}

	return branchID, nil
}

// SetBranchIDFromGitBranching checks if git-branching is enabled and sets KBC_BRANCH_ID
// in the environment before the manifest is loaded. This must be called BEFORE LocalProject().
// Returns the branch ID that was set (empty string for production or if git-branching is not enabled).
func SetBranchIDFromGitBranching(ctx context.Context, fs filesystem.Fs, envs *env.Map, logger log.Logger) (string, error) {
	// Check if manifest exists and has git-branching enabled
	// We read the manifest file directly to check gitBranching config
	// without triggering the full manifest load which would apply the branch ID
	if !projectManifest.ExistsIn(fs) {
		return "", nil
	}

	gitBranchingConfig, err := projectManifest.ReadGitBranchingConfig(ctx, fs)
	if err != nil {
		// If we can't read the config, assume git-branching is not enabled
		return "", nil
	}

	if gitBranchingConfig == nil || !gitBranchingConfig.Enabled {
		return "", nil
	}

	// Check if we're in a git repository
	if !IsGitRepository(ctx, fs) {
		return "", errors.New("git repository not found but git-branching mode is enabled")
	}

	// Get current git branch
	gitBranch, err := CurrentBranch(ctx, fs)
	if err != nil {
		return "", err
	}

	// Check if branch mapping file exists
	if !branchmapping.Exists(ctx, fs) {
		return "", errors.Errorf("git branch not linked to Keboola branch\n\n   Current git branch: %s\n\n   This project uses git-branching mode, which requires each git branch\n   to be explicitly linked to a Keboola branch before sync operations.\n\n   To fix:\n   kbc branch link              # Create and link a new Keboola branch\n   kbc branch link --branch-id=<id>  # Link to existing branch", gitBranch)
	}

	// Load branch mappings
	mappings, err := branchmapping.Load(ctx, fs)
	if err != nil {
		return "", errors.Errorf("failed to load branch mappings: %w", err)
	}

	// Check if current branch is mapped
	mapping, found := mappings.GetMapping(gitBranch)
	if !found {
		return "", errors.Errorf("git branch not linked to Keboola branch\n\n   Current git branch: %s\n\n   This project uses git-branching mode, which requires each git branch\n   to be explicitly linked to a Keboola branch before sync operations.\n\n   To fix:\n   kbc branch link              # Create and link a new Keboola branch\n   kbc branch link --branch-id=<id>  # Link to existing branch", gitBranch)
	}

	// Get the branch ID and set it in environment
	if mapping.IsProduction() {
		// Production branch - no override needed
		logger.Infof(ctx, "Using Keboola branch: %s (production)", mapping.Name)
		return "", nil
	}

	branchID := mapping.GetID()
	logger.Infof(ctx, "Using Keboola branch: %s (ID: %s)", mapping.Name, branchID)

	// Check if KBC_BRANCH_ID is already set
	existingBranchID := envs.Get(BranchIDOverrideENV)
	if existingBranchID != "" && existingBranchID != branchID {
		logger.Warnf(ctx, "Overriding %s=%s with git-branching value %s", BranchIDOverrideENV, existingBranchID, branchID)
	}

	// Set the environment variable
	envs.Set(BranchIDOverrideENV, branchID)

	return branchID, nil
}
