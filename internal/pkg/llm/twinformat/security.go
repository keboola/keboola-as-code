package twinformat

import (
	"context"
	"os/exec"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	// EncryptedPlaceholder is the placeholder for encrypted secrets.
	EncryptedPlaceholder = "***ENCRYPTED***"
	// SecretPrefix is the prefix for secret fields in Keboola.
	SecretPrefix = "#"
)

// SecurityDependencies defines dependencies for the Security module.
type SecurityDependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

// Security handles security-related operations for twin format export.
type Security struct {
	logger    log.Logger
	telemetry telemetry.Telemetry
}

// NewSecurity creates a new Security instance.
func NewSecurity(d SecurityDependencies) *Security {
	return &Security{
		logger:    d.Logger(),
		telemetry: d.Telemetry(),
	}
}

// IsPublicRepository checks if the repository might be public.
// NOTE: This is a placeholder implementation that always returns false (assumes private).
// Actual public repository detection would require GitHub/GitLab API calls with authentication,
// which is beyond the scope of this scaffolding. This method exists to support future
// implementation of automatic sample disabling for public repositories.
func (s *Security) IsPublicRepository(ctx context.Context, repoPath string) (isPublic bool, err error) {
	ctx, span := s.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.security.IsPublicRepository")
	defer span.End(&err)

	// Try to get the remote URL for logging purposes.
	remoteURL, err := s.getGitRemoteURL(ctx, repoPath)
	if err != nil {
		s.logger.Debugf(ctx, "Failed to get git remote URL: %v", err)
		return false, nil
	}

	if remoteURL == "" {
		s.logger.Debugf(ctx, "No git remote URL found, assuming private")
		return false, nil
	}

	// Log detected repository host.
	// NOTE: Actual visibility detection would require API calls.
	// For now, we conservatively assume all repositories are private.
	s.logger.Debugf(ctx, "Repository detected: %s (assuming private - visibility detection not implemented)", remoteURL)
	return false, nil
}

// getGitRemoteURL gets the remote URL of the git repository.
func (s *Security) getGitRemoteURL(ctx context.Context, repoPath string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "remote", "get-url", "origin")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

// EncryptSecrets encrypts all secret fields in a map.
// Secret fields are identified by keys starting with "#".
func (s *Security) EncryptSecrets(data map[string]any) map[string]any {
	return s.encryptSecretsRecursive(data)
}

// encryptSecretsRecursive recursively encrypts secrets in a map.
func (s *Security) encryptSecretsRecursive(data map[string]any) map[string]any {
	result := make(map[string]any)

	for key, value := range data {
		// Check if the key is a secret field.
		if strings.HasPrefix(key, SecretPrefix) {
			result[key] = EncryptedPlaceholder
			continue
		}

		// Recursively process nested maps.
		switch v := value.(type) {
		case map[string]any:
			result[key] = s.encryptSecretsRecursive(v)
		case []any:
			result[key] = s.encryptSecretsInSlice(v)
		default:
			result[key] = value
		}
	}

	return result
}

// encryptSecretsInSlice recursively encrypts secrets in a slice.
func (s *Security) encryptSecretsInSlice(data []any) []any {
	result := make([]any, len(data))

	for i, value := range data {
		switch v := value.(type) {
		case map[string]any:
			result[i] = s.encryptSecretsRecursive(v)
		case []any:
			result[i] = s.encryptSecretsInSlice(v)
		default:
			result[i] = value
		}
	}

	return result
}

// ShouldDisableSamples returns true if samples should be disabled.
// This is based on whether the repository is public.
func (s *Security) ShouldDisableSamples(ctx context.Context, repoPath string) bool {
	isPublic, err := s.IsPublicRepository(ctx, repoPath)
	if err != nil {
		s.logger.Warnf(ctx, "Failed to detect repository visibility: %v", err)
		return false
	}

	if isPublic {
		s.logger.Infof(ctx, "Public repository detected, samples will be disabled for security")
		return true
	}

	return false
}

// SecurityOptions holds security-related options for export.
type SecurityOptions struct {
	// EncryptSecrets enables secret encryption in output.
	EncryptSecrets bool
	// DisableSamples disables sample export.
	DisableSamples bool
	// IsPublicRepo indicates if the repository is public.
	IsPublicRepo bool
}

// DefaultSecurityOptions returns the default security options.
func DefaultSecurityOptions() SecurityOptions {
	return SecurityOptions{
		EncryptSecrets: true,
		DisableSamples: false,
		IsPublicRepo:   false,
	}
}

// DetectSecurityOptions detects security options based on the repository.
func (s *Security) DetectSecurityOptions(ctx context.Context, repoPath string) SecurityOptions {
	opts := DefaultSecurityOptions()

	isPublic, err := s.IsPublicRepository(ctx, repoPath)
	if err != nil {
		s.logger.Warnf(ctx, "Failed to detect repository visibility: %v", err)
		return opts
	}

	opts.IsPublicRepo = isPublic
	if isPublic {
		opts.DisableSamples = true
		s.logger.Infof(ctx, "Public repository detected, samples disabled for security")
	}

	return opts
}
