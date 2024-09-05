// Code generated by goa v3.18.2, DO NOT EDIT.
//
// templates client
//
// Command:
// $ goa gen github.com/keboola/keboola-as-code/api/templates --output
// ./internal/pkg/service/templates/api

package templates

import (
	"context"

	goa "goa.design/goa/v3/pkg"
)

// Client is the "templates" service client.
type Client struct {
	APIRootIndexEndpoint                  goa.Endpoint
	APIVersionIndexEndpoint               goa.Endpoint
	HealthCheckEndpoint                   goa.Endpoint
	RepositoriesIndexEndpoint             goa.Endpoint
	RepositoryIndexEndpoint               goa.Endpoint
	TemplatesIndexEndpoint                goa.Endpoint
	TemplateIndexEndpoint                 goa.Endpoint
	VersionIndexEndpoint                  goa.Endpoint
	InputsIndexEndpoint                   goa.Endpoint
	ValidateInputsEndpoint                goa.Endpoint
	UseTemplateVersionEndpoint            goa.Endpoint
	InstancesIndexEndpoint                goa.Endpoint
	InstanceIndexEndpoint                 goa.Endpoint
	UpdateInstanceEndpoint                goa.Endpoint
	DeleteInstanceEndpoint                goa.Endpoint
	UpgradeInstanceEndpoint               goa.Endpoint
	UpgradeInstanceInputsIndexEndpoint    goa.Endpoint
	UpgradeInstanceValidateInputsEndpoint goa.Endpoint
	GetTaskEndpoint                       goa.Endpoint
}

// NewClient initializes a "templates" service client given the endpoints.
func NewClient(aPIRootIndex, aPIVersionIndex, healthCheck, repositoriesIndex, repositoryIndex, templatesIndex, templateIndex, versionIndex, inputsIndex, validateInputs, useTemplateVersion, instancesIndex, instanceIndex, updateInstance, deleteInstance, upgradeInstance, upgradeInstanceInputsIndex, upgradeInstanceValidateInputs, getTask goa.Endpoint) *Client {
	return &Client{
		APIRootIndexEndpoint:                  aPIRootIndex,
		APIVersionIndexEndpoint:               aPIVersionIndex,
		HealthCheckEndpoint:                   healthCheck,
		RepositoriesIndexEndpoint:             repositoriesIndex,
		RepositoryIndexEndpoint:               repositoryIndex,
		TemplatesIndexEndpoint:                templatesIndex,
		TemplateIndexEndpoint:                 templateIndex,
		VersionIndexEndpoint:                  versionIndex,
		InputsIndexEndpoint:                   inputsIndex,
		ValidateInputsEndpoint:                validateInputs,
		UseTemplateVersionEndpoint:            useTemplateVersion,
		InstancesIndexEndpoint:                instancesIndex,
		InstanceIndexEndpoint:                 instanceIndex,
		UpdateInstanceEndpoint:                updateInstance,
		DeleteInstanceEndpoint:                deleteInstance,
		UpgradeInstanceEndpoint:               upgradeInstance,
		UpgradeInstanceInputsIndexEndpoint:    upgradeInstanceInputsIndex,
		UpgradeInstanceValidateInputsEndpoint: upgradeInstanceValidateInputs,
		GetTaskEndpoint:                       getTask,
	}
}

// APIRootIndex calls the "ApiRootIndex" endpoint of the "templates" service.
func (c *Client) APIRootIndex(ctx context.Context) (err error) {
	_, err = c.APIRootIndexEndpoint(ctx, nil)
	return
}

// APIVersionIndex calls the "ApiVersionIndex" endpoint of the "templates"
// service.
func (c *Client) APIVersionIndex(ctx context.Context) (res *ServiceDetail, err error) {
	var ires any
	ires, err = c.APIVersionIndexEndpoint(ctx, nil)
	if err != nil {
		return
	}
	return ires.(*ServiceDetail), nil
}

// HealthCheck calls the "HealthCheck" endpoint of the "templates" service.
func (c *Client) HealthCheck(ctx context.Context) (res string, err error) {
	var ires any
	ires, err = c.HealthCheckEndpoint(ctx, nil)
	if err != nil {
		return
	}
	return ires.(string), nil
}

// RepositoriesIndex calls the "RepositoriesIndex" endpoint of the "templates"
// service.
func (c *Client) RepositoriesIndex(ctx context.Context, p *RepositoriesIndexPayload) (res *Repositories, err error) {
	var ires any
	ires, err = c.RepositoriesIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Repositories), nil
}

// RepositoryIndex calls the "RepositoryIndex" endpoint of the "templates"
// service.
// RepositoryIndex may return the following errors:
//   - "templates.repositoryNotFound" (type *GenericError): Repository not found error.
//   - error: internal error
func (c *Client) RepositoryIndex(ctx context.Context, p *RepositoryIndexPayload) (res *Repository, err error) {
	var ires any
	ires, err = c.RepositoryIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Repository), nil
}

// TemplatesIndex calls the "TemplatesIndex" endpoint of the "templates"
// service.
// TemplatesIndex may return the following errors:
//   - "templates.repositoryNotFound" (type *GenericError): Repository not found error.
//   - error: internal error
func (c *Client) TemplatesIndex(ctx context.Context, p *TemplatesIndexPayload) (res *Templates, err error) {
	var ires any
	ires, err = c.TemplatesIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Templates), nil
}

// TemplateIndex calls the "TemplateIndex" endpoint of the "templates" service.
// TemplateIndex may return the following errors:
//   - "templates.repositoryNotFound" (type *GenericError): Repository not found error.
//   - "templates.templateNotFound" (type *GenericError): Template not found error.
//   - error: internal error
func (c *Client) TemplateIndex(ctx context.Context, p *TemplateIndexPayload) (res *TemplateDetail, err error) {
	var ires any
	ires, err = c.TemplateIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*TemplateDetail), nil
}

// VersionIndex calls the "VersionIndex" endpoint of the "templates" service.
// VersionIndex may return the following errors:
//   - "templates.repositoryNotFound" (type *GenericError): Repository not found error.
//   - "templates.templateNotFound" (type *GenericError): Template not found error.
//   - "templates.versionNotFound" (type *GenericError): Version not found error.
//   - error: internal error
func (c *Client) VersionIndex(ctx context.Context, p *VersionIndexPayload) (res *VersionDetailExtended, err error) {
	var ires any
	ires, err = c.VersionIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*VersionDetailExtended), nil
}

// InputsIndex calls the "InputsIndex" endpoint of the "templates" service.
// InputsIndex may return the following errors:
//   - "templates.repositoryNotFound" (type *GenericError): Repository not found error.
//   - "templates.templateNotFound" (type *GenericError): Template not found error.
//   - "templates.versionNotFound" (type *GenericError): Version not found error.
//   - error: internal error
func (c *Client) InputsIndex(ctx context.Context, p *InputsIndexPayload) (res *Inputs, err error) {
	var ires any
	ires, err = c.InputsIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Inputs), nil
}

// ValidateInputs calls the "ValidateInputs" endpoint of the "templates"
// service.
// ValidateInputs may return the following errors:
//   - "templates.repositoryNotFound" (type *GenericError): Repository not found error.
//   - "templates.templateNotFound" (type *GenericError): Template not found error.
//   - "templates.versionNotFound" (type *GenericError): Version not found error.
//   - error: internal error
func (c *Client) ValidateInputs(ctx context.Context, p *ValidateInputsPayload) (res *ValidationResult, err error) {
	var ires any
	ires, err = c.ValidateInputsEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*ValidationResult), nil
}

// UseTemplateVersion calls the "UseTemplateVersion" endpoint of the
// "templates" service.
// UseTemplateVersion may return the following errors:
//   - "InvalidInputs" (type *ValidationError): Inputs are not valid.
//   - "templates.repositoryNotFound" (type *GenericError): Repository not found error.
//   - "templates.templateNotFound" (type *GenericError): Template not found error.
//   - "templates.versionNotFound" (type *GenericError): Version not found error.
//   - "templates.projectLocked" (type *ProjectLockedError): Access to branch metadata must be atomic, so only one write operation can run at a time. If this error occurs, the client should make retries, see Retry-After header.
//   - error: internal error
func (c *Client) UseTemplateVersion(ctx context.Context, p *UseTemplateVersionPayload) (res *Task, err error) {
	var ires any
	ires, err = c.UseTemplateVersionEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Task), nil
}

// InstancesIndex calls the "InstancesIndex" endpoint of the "templates"
// service.
// InstancesIndex may return the following errors:
//   - "templates.branchNotFound" (type *GenericError): Branch not found error.
//   - error: internal error
func (c *Client) InstancesIndex(ctx context.Context, p *InstancesIndexPayload) (res *Instances, err error) {
	var ires any
	ires, err = c.InstancesIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Instances), nil
}

// InstanceIndex calls the "InstanceIndex" endpoint of the "templates" service.
// InstanceIndex may return the following errors:
//   - "templates.branchNotFound" (type *GenericError): Branch not found error.
//   - "templates.repositoryNotFound" (type *GenericError): Repository not found error.
//   - "templates.instanceNotFound" (type *GenericError): Instance not found error.
//   - error: internal error
func (c *Client) InstanceIndex(ctx context.Context, p *InstanceIndexPayload) (res *InstanceDetail, err error) {
	var ires any
	ires, err = c.InstanceIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*InstanceDetail), nil
}

// UpdateInstance calls the "UpdateInstance" endpoint of the "templates"
// service.
// UpdateInstance may return the following errors:
//   - "templates.branchNotFound" (type *GenericError): Branch not found error.
//   - "templates.instanceNotFound" (type *GenericError): Instance not found error.
//   - "templates.projectLocked" (type *ProjectLockedError): Access to branch metadata must be atomic, so only one write operation can run at a time. If this error occurs, the client should make retries, see Retry-After header.
//   - error: internal error
func (c *Client) UpdateInstance(ctx context.Context, p *UpdateInstancePayload) (res *InstanceDetail, err error) {
	var ires any
	ires, err = c.UpdateInstanceEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*InstanceDetail), nil
}

// DeleteInstance calls the "DeleteInstance" endpoint of the "templates"
// service.
// DeleteInstance may return the following errors:
//   - "templates.branchNotFound" (type *GenericError): Branch not found error.
//   - "templates.instanceNotFound" (type *GenericError): Instance not found error.
//   - "templates.projectLocked" (type *ProjectLockedError): Access to branch metadata must be atomic, so only one write operation can run at a time. If this error occurs, the client should make retries, see Retry-After header.
//   - error: internal error
func (c *Client) DeleteInstance(ctx context.Context, p *DeleteInstancePayload) (res *Task, err error) {
	var ires any
	ires, err = c.DeleteInstanceEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Task), nil
}

// UpgradeInstance calls the "UpgradeInstance" endpoint of the "templates"
// service.
// UpgradeInstance may return the following errors:
//   - "InvalidInputs" (type *ValidationError): Inputs are not valid.
//   - "templates.templateNotFound" (type *GenericError): Template not found error.
//   - "templates.branchNotFound" (type *GenericError): Branch not found error.
//   - "templates.instanceNotFound" (type *GenericError): Instance not found error.
//   - "templates.versionNotFound" (type *GenericError): Version not found error.
//   - "templates.projectLocked" (type *ProjectLockedError): Access to branch metadata must be atomic, so only one write operation can run at a time. If this error occurs, the client should make retries, see Retry-After header.
//   - error: internal error
func (c *Client) UpgradeInstance(ctx context.Context, p *UpgradeInstancePayload) (res *Task, err error) {
	var ires any
	ires, err = c.UpgradeInstanceEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Task), nil
}

// UpgradeInstanceInputsIndex calls the "UpgradeInstanceInputsIndex" endpoint
// of the "templates" service.
// UpgradeInstanceInputsIndex may return the following errors:
//   - "templates.templateNotFound" (type *GenericError): Template not found error.
//   - "templates.branchNotFound" (type *GenericError): Branch not found error.
//   - "templates.instanceNotFound" (type *GenericError): Instance not found error.
//   - "templates.versionNotFound" (type *GenericError): Version not found error.
//   - error: internal error
func (c *Client) UpgradeInstanceInputsIndex(ctx context.Context, p *UpgradeInstanceInputsIndexPayload) (res *Inputs, err error) {
	var ires any
	ires, err = c.UpgradeInstanceInputsIndexEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Inputs), nil
}

// UpgradeInstanceValidateInputs calls the "UpgradeInstanceValidateInputs"
// endpoint of the "templates" service.
// UpgradeInstanceValidateInputs may return the following errors:
//   - "templates.templateNotFound" (type *GenericError): Template not found error.
//   - "templates.branchNotFound" (type *GenericError): Branch not found error.
//   - "templates.instanceNotFound" (type *GenericError): Instance not found error.
//   - "templates.versionNotFound" (type *GenericError): Version not found error.
//   - error: internal error
func (c *Client) UpgradeInstanceValidateInputs(ctx context.Context, p *UpgradeInstanceValidateInputsPayload) (res *ValidationResult, err error) {
	var ires any
	ires, err = c.UpgradeInstanceValidateInputsEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*ValidationResult), nil
}

// GetTask calls the "GetTask" endpoint of the "templates" service.
// GetTask may return the following errors:
//   - "templates.taskNotFound" (type *GenericError): Task not found error.
//   - error: internal error
func (c *Client) GetTask(ctx context.Context, p *GetTaskPayload) (res *Task, err error) {
	var ires any
	ires, err = c.GetTaskEndpoint(ctx, p)
	if err != nil {
		return
	}
	return ires.(*Task), nil
}
