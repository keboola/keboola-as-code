package datagateway

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const dataGatewayComponentID = keboola.ComponentID("keboola.app-data-gateway")

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

type dataGatewayMapper struct {
	dependencies
	state  *state.State
	logger log.Logger
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

// generateRSAKeyPairPEM generates a 2048-bit RSA key pair suitable for Snowflake key-pair auth.
// The private key is encoded as PKCS#8 PEM without encryption, and the public key is PKIX PEM.
// This is copied from pkg/lib/operation/dbt/init/operation.go to avoid circular dependencies.
func generateRSAKeyPairPEM() (privateKeyPEM string, publicKeyPEM string, err error) {
	// Generate private key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// Marshal private key to PKCS#8
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", err
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Bytes})

	// Marshal public key to PKIX
	pubBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", "", err
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes})

	return string(privPEM), string(pubPEM), nil
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

// ptr returns a pointer to the given value.
func ptr[T any](v T) *T {
	return &v
}

// ensureWorkspaceForConfig ensures a workspace exists for the given config, creating one if necessary.
func (m *dataGatewayMapper) ensureWorkspaceForConfig(ctx context.Context, config *model.Config) error {
	api := m.KeboolaProjectAPI()

	// Check if config has an ID (required for creating config workspace)
	if config.ID == "" {
		m.logger.Debugf(ctx, `Skipping data-gateway config "%s" without ID - cannot create workspace for local-only configs`, config.Name)
		return nil
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
	_, publicKeyPEM, err := generateRSAKeyPairPEM()
	if err != nil {
		return errors.Errorf(`cannot generate keypair for config "%s": %w`, config.Name, err)
	}

	// Create configuration workspace
	// Data gateway workspaces require useCase to be set to "reader" for read-only access.
	networkPolicy := "user"
	payload := &keboola.StorageConfigWorkspacePayload{
		StorageWorkspacePayload: keboola.StorageWorkspacePayload{
			Backend:               keboola.StorageWorkspaceBackendSnowflake,
			BackendSize:           ptr(keboola.StorageWorkspaceBackendSizeMedium),
			NetworkPolicy:         &networkPolicy,
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
func setNestedIfDifferent(config *model.Config, path string, value any) bool {
	current, found, _ := config.Content.GetNested(path)
	if found && reflect.DeepEqual(current, value) {
		return false
	}
	_ = config.Content.SetNested(path, value)
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
