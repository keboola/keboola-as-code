package push

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const dataGatewayComponentID = "keboola.app-data-gateway"

// ensureDataGatewayWorkspaces creates missing workspaces for data-gateway configurations before push.
func ensureDataGatewayWorkspaces(ctx context.Context, projectState *project.State, d dependencies) error {
	logger := d.Logger()
	api := d.KeboolaProjectAPI()

	// Cache keypairs per branch to avoid generating multiple times
	keypairCache := make(map[keboola.BranchID]*keypairInfo)

	// Iterate through all branches
	for _, branchState := range projectState.Branches() {
		if !branchState.HasLocalState() {
			continue
		}

		branch := branchState.Local
		configs := projectState.LocalObjects().ConfigsFrom(branch.BranchKey)

		// Find data-gateway configurations
		for _, config := range configs {
			if config.ComponentID != dataGatewayComponentID {
				continue
			}

			// Check if workspace is missing
			workspaceID, found := config.Content.GetNested("parameters.db.workspaceId")
			if found && workspaceID != nil && workspaceID != "" {
				// Workspace already exists, skip
				continue
			}

			// Check if config has an ID (required for creating config workspace)
			if config.ID == "" {
				logger.Warnf(ctx, `Skipping data-gateway config "%s" without ID - cannot create workspace for local-only configs`, config.Name)
				continue
			}

			logger.Infof(ctx, `Creating workspace for data-gateway config "%s"...`, config.Name)

			// Get or generate keypair for this branch
			keypair, err := getOrGenerateKeypair(ctx, branch.ID, keypairCache)
			if err != nil {
				return errors.Errorf(`cannot generate keypair for branch %d: %w`, branch.ID, err)
			}

			// Create configuration workspace
			networkPolicy := "user"
			payload := &keboola.StorageWorkspacePayload{
				Backend:               keboola.StorageWorkspaceBackendSnowflake,
				BackendSize:           ptr(keboola.StorageWorkspaceBackendSizeMedium),
				NetworkPolicy:         &networkPolicy,
				ReadOnlyStorageAccess: true,
				LoginType:             keboola.StorageWorkspaceLoginTypeSnowflakeServiceKeypair,
				PublicKey:             &keypair.publicKeyPEM,
			}

			workspace, err := api.CreateConfigWorkspaceRequest(branch.ID, config.ComponentID, config.ID, payload).Send(ctx)
			if err != nil {
				return errors.Errorf(`cannot create workspace for config "%s": %w`, config.Name, err)
			}

			logger.Infof(ctx, `Created workspace %d for config "%s"`, workspace.ID, config.Name)

			// Backfill configuration with workspace details
			if err := backfillWorkspaceDetails(config, workspace); err != nil {
				return errors.Errorf(`cannot backfill workspace details for config "%s": %w`, config.Name, err)
			}

			logger.Infof(ctx, `Backfilled workspace details for config "%s"`, config.Name)
		}
	}

	return nil
}

type keypairInfo struct {
	privateKeyPEM string
	publicKeyPEM  string
}

// getOrGenerateKeypair gets or generates an RSA keypair for a branch.
func getOrGenerateKeypair(ctx context.Context, branchID keboola.BranchID, cache map[keboola.BranchID]*keypairInfo) (*keypairInfo, error) {
	if keypair, found := cache[branchID]; found {
		return keypair, nil
	}

	privateKeyPEM, publicKeyPEM, err := generateRSAKeyPairPEM()
	if err != nil {
		return nil, err
	}

	keypair := &keypairInfo{
		privateKeyPEM: privateKeyPEM,
		publicKeyPEM:  publicKeyPEM,
	}
	cache[branchID] = keypair
	return keypair, nil
}

// generateRSAKeyPairPEM generates a 2048-bit RSA key pair suitable for Snowflake key-pair auth.
// The private key is encoded as PKCS#8 PEM without encryption, and the public key is PKIX PEM.
// This is copied from pkg/lib/operation/dbt/init/operation.go to avoid circular dependencies.
func generateRSAKeyPairPEM() (string, string, error) {
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
func backfillWorkspaceDetails(config *model.Config, workspace *keboola.StorageWorkspace) error {
	// Set workspace ID
	config.Content.SetNested(workspace.ID, "parameters", "db", "workspaceId")

	// Set connection details from workspace
	if workspace.Connection != nil {
		if workspace.Connection.Host != "" {
			config.Content.SetNested(workspace.Connection.Host, "parameters", "db", "host")
		}
		if workspace.Connection.User != "" {
			config.Content.SetNested(workspace.Connection.User, "parameters", "db", "user")
		}
		if workspace.Connection.Database != "" {
			config.Content.SetNested(workspace.Connection.Database, "parameters", "db", "database")
		}
		if workspace.Connection.Schema != "" {
			config.Content.SetNested(workspace.Connection.Schema, "parameters", "db", "schema")
		}
		if workspace.Connection.Warehouse != "" {
			config.Content.SetNested(workspace.Connection.Warehouse, "parameters", "db", "warehouse")
		}
		if workspace.Connection.Role != "" {
			config.Content.SetNested(workspace.Connection.Role, "parameters", "db", "role")
		}
		if workspace.Connection.Account != "" {
			config.Content.SetNested(workspace.Connection.Account, "parameters", "db", "account")
		}
		if workspace.Connection.Region != "" {
			config.Content.SetNested(workspace.Connection.Region, "parameters", "db", "region")
		}
	}

	// Set login type
	config.Content.SetNested("snowflake-service-keypair", "parameters", "db", "loginType")

	return nil
}

// ptr returns a pointer to the given value.
func ptr[T any](v T) *T {
	return &v
}
