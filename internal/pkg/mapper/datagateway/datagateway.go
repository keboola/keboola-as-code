package datagateway

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"

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
func backfillWorkspaceDetails(config *model.Config, workspace *keboola.StorageWorkspace) {
	// Set workspace ID
	_ = config.Content.SetNested("parameters.db.workspaceId", workspace.ID)

	// Set connection details from workspace
	details := workspace.StorageWorkspaceDetails
	if details.Host != nil && *details.Host != "" {
		_ = config.Content.SetNested("parameters.db.host", *details.Host)
	}
	if details.User != nil && *details.User != "" {
		_ = config.Content.SetNested("parameters.db.user", *details.User)
	}
	if details.Database != nil && *details.Database != "" {
		_ = config.Content.SetNested("parameters.db.database", *details.Database)
	}
	if details.Schema != nil && *details.Schema != "" {
		_ = config.Content.SetNested("parameters.db.schema", *details.Schema)
	}
	if details.Warehouse != nil && *details.Warehouse != "" {
		_ = config.Content.SetNested("parameters.db.warehouse", *details.Warehouse)
	}
	if details.Role != nil && *details.Role != "" {
		_ = config.Content.SetNested("parameters.db.role", *details.Role)
	}
	if details.Account != nil && *details.Account != "" {
		_ = config.Content.SetNested("parameters.db.account", *details.Account)
	}
	if details.Region != nil && *details.Region != "" {
		_ = config.Content.SetNested("parameters.db.region", *details.Region)
	}

	// Set login type
	_ = config.Content.SetNested("parameters.db.loginType", "snowflake-service-keypair")
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
	networkPolicy := "user"
	payload := &keboola.StorageWorkspacePayload{
		Backend:               keboola.StorageWorkspaceBackendSnowflake,
		BackendSize:           ptr(keboola.StorageWorkspaceBackendSizeMedium),
		NetworkPolicy:         &networkPolicy,
		ReadOnlyStorageAccess: true,
		LoginType:             keboola.StorageWorkspaceLoginTypeSnowflakeServiceKeypair,
		PublicKey:             &publicKeyPEM,
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
