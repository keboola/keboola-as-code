package datagateway

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/crypto"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const dataGatewayComponentID = keboola.ComponentID("keboola.app-data-gateway")

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

type dataGatewayMapper struct {
	dependencies
	state     *state.State
	logger    log.Logger
	projectID keboola.ProjectID // Cached project ID to avoid repeated API calls
}

func NewMapper(s *state.State, d dependencies) *dataGatewayMapper {
	return &dataGatewayMapper{
		dependencies: d,
		state:        s,
		logger:       s.Logger(),
	}
}

func (m *dataGatewayMapper) isDataGatewayConfigKey(key model.Key) bool {
	configKey, ok := key.(model.ConfigKey)
	if !ok {
		return false
	}
	return configKey.ComponentID == dataGatewayComponentID
}

// backfillWorkspaceDetails updates the configuration content with workspace details from the API response.
// It returns true when at least one field has been updated.
func backfillWorkspaceDetails(config *model.Config, workspace *keboola.StorageWorkspace) bool {
	if config == nil || workspace == nil {
		return false
	}

	changed := setWorkspaceID(config, workspace.ID)

	details := workspace.StorageWorkspaceDetails
	changed = setStringIfPresent(config, "parameters.db.host", details.Host) || changed
	changed = setStringIfPresent(config, "parameters.db.user", details.User) || changed
	changed = setStringIfPresent(config, "parameters.db.database", details.Database) || changed
	changed = setStringIfPresent(config, "parameters.db.schema", details.Schema) || changed
	changed = setStringIfPresent(config, "parameters.db.warehouse", details.Warehouse) || changed
	changed = setStringIfPresent(config, "parameters.db.role", details.Role) || changed
	changed = setStringIfPresent(config, "parameters.db.account", details.Account) || changed
	changed = setStringIfPresent(config, "parameters.db.region", details.Region) || changed

	changed = setNestedIfDifferent(config, "parameters.db.loginType", "snowflake-service-keypair") || changed
	return changed
}

// ensureWorkspaceForConfig ensures a workspace exists for the given config, creating one if necessary.
func (m *dataGatewayMapper) ensureWorkspaceForConfig(ctx context.Context, config *model.Config) error {
	api := m.KeboolaProjectAPI()

	// Check if config has an ID (required for creating config workspace)
	if config.ID == "" {
		m.logger.Debugf(ctx, `Skipping data-gateway config "%s" without ID - cannot create workspace for local-only configs`, config.Name)
		return nil
	}

	// Cache project ID from manifest if not already cached
	// The project ID is available in the manifest, so we can get it from there
	if m.projectID == 0 {
		manifest := m.state.Manifest()
		if projectManifest, ok := manifest.(*projectManifest.Manifest); ok {
			m.projectID = projectManifest.ProjectID()
		}
	}

	// List existing workspaces for this config
	workspaces, err := api.ListConfigWorkspacesRequest(config.BranchID, config.ComponentID, config.ID).Send(ctx)
	if err != nil {
		// If configuration doesn't exist yet (not pushed to remote), skip workspace creation.
		// The workspace will be created after the configuration is saved to remote.
		var apiErr *keboola.StorageError
		if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.configuration.notFound" {
			m.logger.Debugf(ctx, `Config "%s" does not exist in remote yet, workspace will be created after config is saved`, config.Name)
			return nil
		}
		return errors.Errorf(`cannot list workspaces for config "%s": %w`, config.Name, err)
	}

	// Cache project ID from workspace response if available and not yet cached
	// Note: Workspaces don't directly expose project ID, so we rely on the manifest
	// which is checked earlier in this function.

	// If workspace already exists, use it
	if len(*workspaces) > 0 {
		workspace := (*workspaces)[0]
		m.logger.Debugf(ctx, `Using existing workspace %d for config "%s"`, workspace.ID, config.Name)
		backfillWorkspaceDetails(config, workspace)
		return nil
	}

	// No workspace exists, create one
	m.logger.Infof(ctx, `Creating workspace for data-gateway config "%s"...`, config.Name)

	// Generate keypair
	privateKeyPEM, publicKeyPEM, err := crypto.GenerateRSAKeyPairPEM()
	if err != nil {
		return errors.Errorf(`cannot generate keypair for config "%s": %w`, config.Name, err)
	}

	// Store private key to /tmp/<project_id>/<configuration-id>
	// Private key storage is critical for workspace functionality.
	// If project ID is not available, fail immediately to prevent creating an unusable workspace.
	if m.projectID == 0 {
		return errors.Errorf(`project ID not available for config "%s", cannot store private key - workspace would be unusable`, config.Name)
	}

	// If storage fails, the workspace will be created but won't be usable.
	// Fail the operation to prevent creating an unusable workspace.
	if err := m.storePrivateKey(ctx, config, privateKeyPEM); err != nil {
		return errors.Errorf(`cannot store private key for config "%s": %w`, config.Name, err)
	}

	// Create configuration workspace
	// Data gateway workspaces require useCase to be set to "reader" for read-only access.
	payload := &keboola.StorageConfigWorkspacePayload{
		StorageWorkspacePayload: keboola.StorageWorkspacePayload{
			Backend:               keboola.StorageWorkspaceBackendSnowflake,
			ReadOnlyStorageAccess: true,
			LoginType:             keboola.StorageWorkspaceLoginTypeSnowflakeServiceKeypair,
			PublicKey:             &publicKeyPEM,
		},
		UseCase: keboola.StorageWorkspaceUseCaseReader,
	}

	workspace, err := api.CreateConfigWorkspaceRequest(config.BranchID, config.ComponentID, config.ID, payload).Send(ctx)
	if err != nil {
		return errors.Errorf(`cannot create workspace for config "%s": %w`, config.Name, err)
	}

	// Project ID should already be cached from manifest at the start of this function
	// If it's still not set, try to get it from the manifest again as a fallback
	if m.projectID == 0 {
		manifest := m.state.Manifest()
		if projectManifest, ok := manifest.(*projectManifest.Manifest); ok {
			m.projectID = projectManifest.ProjectID()
		}
	}

	m.logger.Infof(ctx, `Created workspace %d for config "%s"`, workspace.ID, config.Name)

	// Backfill configuration with workspace details
	backfillWorkspaceDetails(config, workspace)

	return nil
}

// setStringIfPresent writes non-empty pointer values to the requested path.
func setStringIfPresent(config *model.Config, path string, value *string) bool {
	if value == nil || *value == "" {
		return false
	}
	return setNestedIfDifferent(config, path, *value)
}

// setNestedIfDifferent writes the value only if it differs from the existing state.
// Returns true if the value was set successfully, false if unchanged or if setting failed.
// If SetNested fails, returns false to prevent incorrectly reporting success.
// Note: The error from SetNested is not surfaced to callers due to function signature limitations.
func setNestedIfDifferent(config *model.Config, path string, value any) bool {
	current, found, _ := config.Content.GetNested(path)
	if found && reflect.DeepEqual(current, value) {
		return false
	}
	// Handle SetNested error to prevent silent failures.
	// If the set operation fails, return false to indicate no change was made.
	if err := config.Content.SetNested(path, value); err != nil {
		return false
	}
	return true
}

// needsWorkspaceDetails returns true when workspace metadata in configuration is missing/incomplete.
func needsWorkspaceDetails(config *model.Config) bool {
	requiredStringPaths := []string{
		"parameters.db.host",
		"parameters.db.user",
		"parameters.db.database",
		"parameters.db.schema",
		"parameters.db.warehouse",
		"parameters.db.role",
		"parameters.db.account",
		"parameters.db.region",
	}

	loginTypeValue, found, _ := config.Content.GetNested("parameters.db.loginType")
	if !found || loginTypeValue == nil {
		return true
	}
	if str, ok := loginTypeValue.(string); !ok || str != "snowflake-service-keypair" {
		return true
	}

	for _, path := range requiredStringPaths {
		value, found, _ := config.Content.GetNested(path)
		if !found || value == nil {
			return true
		}
		str, ok := value.(string)
		if !ok || str == "" {
			return true
		}
	}

	return false
}

// setWorkspaceID stores workspace ID as json.Number so it matches how local JSON is parsed.
func setWorkspaceID(config *model.Config, workspaceID uint64) bool {
	number := json.Number(strconv.FormatUint(workspaceID, 10))
	return setNestedIfDifferent(config, "parameters.db.workspaceId", number)
}

// storePrivateKey saves the private key to /tmp/<project_id>/<configuration-id>/private_key.pem.
// The directory structure is created if it doesn't exist.
func (m *dataGatewayMapper) storePrivateKey(ctx context.Context, config *model.Config, privateKeyPEM string) error {
	// Get cached project ID (must be set before calling this method)
	if m.projectID == 0 {
		return errors.New("project ID not set, ensure ensureWorkspaceForConfig sets it first")
	}
	projectID := m.projectID

	// Get configuration ID
	configID := config.ID
	if configID == "" {
		return errors.New("config ID is empty")
	}

	// Create OS filesystem instance for /tmp
	// Use aferofs.NewLocalFs to get a proper filesystem.Fs interface
	tmpFs, err := aferofs.NewLocalFs("/tmp")
	if err != nil {
		return errors.Errorf("cannot create filesystem for /tmp: %w", err)
	}

	// Build relative directory path: <project_id>/<configuration-id>
	// Use filesystem.Join for filesystem operations (uses forward slashes)
	projectDirPath := filesystem.Join(fmt.Sprintf("%d", projectID))
	relativeDirPath := filesystem.Join(projectDirPath, string(configID))

	// Create project directory if it doesn't exist
	if !tmpFs.Exists(ctx, projectDirPath) {
		if err := tmpFs.Mkdir(ctx, projectDirPath); err != nil {
			absProjectPath := filesystem.Join("/tmp", fmt.Sprintf("%d", projectID))
			return errors.Errorf("cannot create project directory %s: %w", absProjectPath, err)
		}
	}

	// Create configuration directory if it doesn't exist
	if !tmpFs.Exists(ctx, relativeDirPath) {
		if err := tmpFs.Mkdir(ctx, relativeDirPath); err != nil {
			absDirPath := filesystem.Join("/tmp", fmt.Sprintf("%d", projectID), string(configID))
			return errors.Errorf("cannot create directory %s: %w", absDirPath, err)
		}
		absDirPath := filesystem.Join("/tmp", fmt.Sprintf("%d", projectID), string(configID))
		m.logger.Debugf(ctx, `Created directory "%s" for private key storage`, absDirPath)
	}

	// Write private key file with restrictive permissions (0600 - owner read/write only)
	// This ensures the private key is not readable by other users on the system.
	privateKeyPath := filesystem.Join(relativeDirPath, "private_key.pem")
	// Use OpenFile directly to set restrictive permissions (0600) instead of default 0644
	fd, err := tmpFs.OpenFile(ctx, privateKeyPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		absKeyPath := filesystem.Join("/tmp", fmt.Sprintf("%d", projectID), string(configID), "private_key.pem")
		return errors.Errorf("cannot open private key file %s: %w", absKeyPath, err)
	}
	if _, err := fd.WriteString(privateKeyPEM); err != nil {
		absKeyPath := filesystem.Join("/tmp", fmt.Sprintf("%d", projectID), string(configID), "private_key.pem")
		_ = fd.Close()
		return errors.Errorf("cannot write private key to %s: %w", absKeyPath, err)
	}
	if err := fd.Close(); err != nil {
		absKeyPath := filesystem.Join("/tmp", fmt.Sprintf("%d", projectID), string(configID), "private_key.pem")
		return errors.Errorf("cannot close private key file %s: %w", absKeyPath, err)
	}

	absKeyPath := filesystem.Join("/tmp", fmt.Sprintf("%d", projectID), string(configID), "private_key.pem")
	m.logger.Infof(ctx, `Stored private key for config "%s" to "%s"`, config.Name, absKeyPath)
	return nil
}

// normalizeWorkspaceID rewrites workspaceId to json.Number when possible.
// This ensures consistent type representation across different JSON parsing scenarios.
func normalizeWorkspaceID(config *model.Config) bool {
	value, found, _ := config.Content.GetNested("parameters.db.workspaceId")
	if !found || value == nil {
		return false
	}

	switch v := value.(type) {
	case json.Number:
		return false
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return false
		}
		if _, err := strconv.ParseFloat(trimmed, 64); err != nil {
			return false
		}
		return setNestedIfDifferent(config, "parameters.db.workspaceId", json.Number(trimmed))
	case float64:
		if math.IsNaN(v) {
			return false
		}
		return setNestedIfDifferent(config, "parameters.db.workspaceId", json.Number(strconv.FormatFloat(v, 'f', -1, 64)))
	case float32:
		if math.IsNaN(float64(v)) {
			return false
		}
		return setNestedIfDifferent(config, "parameters.db.workspaceId", json.Number(strconv.FormatFloat(float64(v), 'f', -1, 32)))
	case int, int32, int64:
		return setNestedIfDifferent(config, "parameters.db.workspaceId", json.Number(fmt.Sprintf("%d", v)))
	case uint, uint32, uint64:
		return setNestedIfDifferent(config, "parameters.db.workspaceId", json.Number(fmt.Sprintf("%d", v)))
	default:
		return false
	}
}
