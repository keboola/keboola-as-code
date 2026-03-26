// Package sandbox provides Python/R workspace types and operations that were removed
// from keboola-sdk-go v2.17.1-0.20260326112115-8a6ce0872c8a. The SDK now exposes
// DataScienceApp for listing/fetching workspaces; this package bridges the gap for
// existing callers without requiring a full migration.
package sandbox

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// SandboxWorkspace represents a Python/R sandbox workspace.
// Host/User/Password are not populated when sourced from DataScienceApp;
// use StorageWorkspaceCreateCredentialsRequest to retrieve them when needed.
type SandboxWorkspace struct {
	ID          keboola.SandboxWorkspaceID
	Type        keboola.SandboxWorkspaceType
	Size        string
	User        string
	Host        string
	URL         string
	Password    string //nolint:gosec
	Details     *SandboxWorkspaceDetails
	Credentials *SandboxWorkspaceCredentials
}

// SandboxWorkspaceDetails holds connection details for a SQL workspace.
type SandboxWorkspaceDetails struct {
	Connection struct {
		Database  string
		Schema    string
		Warehouse string
	}
}

// SandboxWorkspaceCredentials contains BigQuery service-account credentials.
type SandboxWorkspaceCredentials struct {
	Type                    string
	ProjectID               string
	PrivateKeyID            string
	ClientEmail             string
	ClientID                string
	AuthURI                 string
	TokenURI                string
	AuthProviderX509CertURL string
	ClientX509CertURL       string
	PrivateKey              string //nolint:gosec
}

// SandboxWorkspaceWithConfig pairs a workspace instance with its keboola.sandboxes config.
type SandboxWorkspaceWithConfig struct {
	SandboxWorkspace *SandboxWorkspace
	Config           *keboola.Config
}

func (v SandboxWorkspaceWithConfig) String() string {
	if keboola.SandboxWorkspaceSupportsSizes(v.SandboxWorkspace.Type) {
		return fmt.Sprintf("ID: %s, Type: %s, Size: %s, Name: %s",
			v.SandboxWorkspace.ID, v.SandboxWorkspace.Type, v.SandboxWorkspace.Size, v.Config.Name)
	}
	return fmt.Sprintf("ID: %s, Type: %s, Name: %s",
		v.SandboxWorkspace.ID, v.SandboxWorkspace.Type, v.Config.Name)
}

// createParams holds options for the keboola.sandboxes "create" queue job.
type createParams struct {
	Type             keboola.SandboxWorkspaceType
	Shared           bool
	ExpireAfterHours uint64
	Size             string
	ImageVersion     string
	PublicKey        string
	LoginType        string
}

func (p createParams) toMap() map[string]any {
	m := map[string]any{
		"task":                 "create",
		"type":                 p.Type,
		"shared":               p.Shared,
		"expirationAfterHours": p.ExpireAfterHours,
	}
	if len(p.Size) > 0 {
		m["size"] = p.Size
	}
	if len(p.ImageVersion) > 0 {
		m["imageVersion"] = p.ImageVersion
	}
	if len(p.PublicKey) > 0 {
		m["publicKey"] = p.PublicKey
		m["loginType"] = p.LoginType
	}
	return m
}

// CreateSandboxWorkspaceOption configures workspace creation.
type CreateSandboxWorkspaceOption func(p *createParams)

// WithSize sets the backend size (small, medium, large) for Python/R workspaces.
func WithSize(v string) CreateSandboxWorkspaceOption {
	return func(p *createParams) { p.Size = v }
}

// WithPublicKey configures keypair authentication with the given public key PEM.
func WithPublicKey(v string) CreateSandboxWorkspaceOption {
	return func(p *createParams) {
		p.PublicKey = v
		p.LoginType = "snowflake-person-keypair"
	}
}

// WithShared marks the workspace as shared.
func WithShared(v bool) CreateSandboxWorkspaceOption {
	return func(p *createParams) { p.Shared = v }
}

// WithExpireAfterHours sets automatic expiry.
func WithExpireAfterHours(v uint64) CreateSandboxWorkspaceOption {
	return func(p *createParams) { p.ExpireAfterHours = v }
}

// GetSandboxWorkspaceID reads the workspace instance ID from a keboola.sandboxes config.
// The ID is stored at parameters.id in the config content.
func GetSandboxWorkspaceID(c *keboola.Config) (keboola.SandboxWorkspaceID, error) {
	id, found, err := c.Content.GetNested("parameters.id")
	if err != nil {
		return "", err
	}
	if !found {
		return "", errors.Errorf("config is missing parameters.id")
	}
	out, ok := id.(string)
	if !ok {
		return "", errors.Errorf("config.parameters.id is not a string")
	}
	return keboola.SandboxWorkspaceID(out), nil
}

// CreateSandboxWorkspace creates a keboola.sandboxes config, runs the creation queue job,
// waits for completion, then returns the workspace joined with its config.
func CreateSandboxWorkspace(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	branchID keboola.BranchID,
	name string,
	wsType keboola.SandboxWorkspaceType,
	opts ...CreateSandboxWorkspaceOption,
) (*SandboxWorkspaceWithConfig, error) {
	emptyConfig, err := api.CreateSandboxWorkspaceConfigRequest(branchID, name).Send(ctx)
	if err != nil {
		return nil, err
	}

	p := createParams{Type: wsType}
	for _, opt := range opts {
		opt(&p)
	}

	req := api.NewCreateJobRequest(keboola.SandboxWorkspacesComponent).
		WithConfig(emptyConfig.ID).
		WithConfigData(map[string]any{"parameters": p.toMap()}).
		Build().
		WithOnSuccess(func(ctx context.Context, result *keboola.QueueJob) error {
			return api.WaitForQueueJob(ctx, result.ID)
		})
	if _, err = request.NewAPIRequest(request.NoResult{}, req).Send(ctx); err != nil {
		return nil, err
	}

	return GetSandboxWorkspace(ctx, api, branchID, emptyConfig.ID)
}

// DeleteSandboxWorkspace deletes the workspace instance via queue job, then deletes the config.
func DeleteSandboxWorkspace(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	branchID keboola.BranchID,
	configID keboola.ConfigID,
	workspaceID keboola.SandboxWorkspaceID,
) error {
	if _, err := api.DeleteSandboxWorkspaceJobRequest(workspaceID).Send(ctx); err != nil {
		return err
	}
	_, err := api.DeleteSandboxWorkspaceConfigRequest(branchID, configID).Send(ctx)
	return err
}

// GetSandboxWorkspace fetches the config then the DataScienceApp, returning both joined.
func GetSandboxWorkspace(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	branchID keboola.BranchID,
	configID keboola.ConfigID,
) (*SandboxWorkspaceWithConfig, error) {
	config, err := api.GetSandboxWorkspaceConfigRequest(branchID, configID).Send(ctx)
	if err != nil {
		return nil, err
	}

	workspaceID, err := GetSandboxWorkspaceID(config)
	if err != nil {
		return nil, err
	}

	app, err := api.GetDataScienceAppRequest(keboola.DataScienceAppID(workspaceID)).Send(ctx)
	if err != nil {
		return nil, err
	}

	return &SandboxWorkspaceWithConfig{
		SandboxWorkspace: dataScienceAppToWorkspace(app),
		Config:           config,
	}, nil
}

// ListSandboxWorkspaces fetches Python/R workspaces for a branch in parallel with configs,
// joining by DataScienceApp.ConfigID. It also returns all sandbox configs so callers
// can look up SQL workspace names without a second API call.
func ListSandboxWorkspaces(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	branchID keboola.BranchID,
) ([]*SandboxWorkspaceWithConfig, []*keboola.Config, error) {
	var configs []*keboola.Config
	var apps []*keboola.DataScienceApp
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	var err error

	wg.Go(func() {
		data, e := api.ListSandboxWorkspaceConfigRequest(branchID).Send(ctx)
		if e != nil {
			mu.Lock()
			defer mu.Unlock()
			err = multierror.Append(err, e)
			return
		}
		configs = *data
	})

	wg.Go(func() {
		data, e := api.ListDataScienceAppsRequest(
			keboola.WithDataScienceAppsComponentID(keboola.ComponentID(keboola.SandboxWorkspacesComponent)),
			keboola.WithDataScienceAppsBranchID(branchID.String()),
			keboola.WithDataScienceAppsType(keboola.DataScienceAppTypePython, keboola.DataScienceAppTypeR),
		).Send(ctx)
		if e != nil {
			mu.Lock()
			defer mu.Unlock()
			err = multierror.Append(err, e)
			return
		}
		apps = *data
	})

	wg.Wait()
	if err != nil {
		return nil, nil, err
	}

	appsByConfigID := make(map[string]*keboola.DataScienceApp, len(apps))
	for _, app := range apps {
		appsByConfigID[app.ConfigID] = app
	}

	out := make([]*SandboxWorkspaceWithConfig, 0)
	for _, config := range configs {
		app, found := appsByConfigID[config.ID.String()]
		if !found {
			continue
		}
		out = append(out, &SandboxWorkspaceWithConfig{
			SandboxWorkspace: dataScienceAppToWorkspace(app),
			Config:           config,
		})
	}
	return out, configs, nil
}

// WorkspaceFromStorage constructs a SandboxWorkspace from StorageWorkspace credentials,
// used when credentials come from an editor session (SQL workspaces).
func WorkspaceFromStorage(sw *keboola.StorageWorkspace, wsType keboola.SandboxWorkspaceType) *SandboxWorkspace {
	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	details := &SandboxWorkspaceDetails{}
	details.Connection.Database = deref(sw.StorageWorkspaceDetails.Database)
	details.Connection.Schema = deref(sw.StorageWorkspaceDetails.Schema)
	details.Connection.Warehouse = deref(sw.StorageWorkspaceDetails.Warehouse)
	return &SandboxWorkspace{
		Type:    wsType,
		Host:    deref(sw.StorageWorkspaceDetails.Host),
		User:    deref(sw.StorageWorkspaceDetails.User),
		Details: details,
	}
}

func dataScienceAppToWorkspace(app *keboola.DataScienceApp) *SandboxWorkspace {
	return &SandboxWorkspace{
		ID:      keboola.SandboxWorkspaceID(app.ID),
		Type:    keboola.SandboxWorkspaceType(app.Type),
		Size:    app.Size,
		URL:     app.URL,
		Details: &SandboxWorkspaceDetails{}, // always non-nil; fields are empty for Python/R
	}
}
