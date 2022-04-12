package remote

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type saveContext struct {
	*uow
	parentCtx state.SaveContext
}

func (c *saveContext) save() {
	// Invoke mapper
	mapperCtx, err := c.mapper.MapBeforeRemoteSave(c.parentCtx.Object, c.parentCtx.ChangedFields)
	if err != nil {
		c.errs.Append(err)
		return
	}

	// Relations are stored on the API side in config/row configuration.
	// This is ensured by the mapper layer.
	if mapperCtx.ChangedFields().Has(`relations`) {
		mapperCtx.ChangedFields().Add(`configuration`)
		mapperCtx.ChangedFields().Remove(`relations`)
	}

	// Branch cannot be created, it must be cloned
	if v, ok := c.parentCtx.Object.(*model.Branch); ok && !c.parentCtx.ObjectExists {
		c.errs.Append(fmt.Errorf(`branch "%d" (%s) cannot be created, it must be created as clone of the main branch directly in the project`, v.BranchId, v.Name))
		return
	}

	// Set changeDescription
	switch v := c.parentCtx.Object.(type) {
	case *model.Config:
		v.ChangeDescription = c.changeDescription
		c.parentCtx.ChangedFields.Add("changeDescription")
	case *model.ConfigRow:
		v.ChangeDescription = c.changeDescription
		c.parentCtx.ChangedFields.Add("changeDescription")
	}

	// Should metadata be set?
	setMetadata := !c.parentCtx.ObjectExists || c.parentCtx.ChangedFields.Has("metadata")
	var setMetadataRequest *client.Request
	if setMetadata {
		c.parentCtx.ChangedFields.Remove("metadata")
		setMetadataRequest = c.storageApi.AppendMetadataRequest(c.parentCtx.Object)
	}

	// Create request
	saveRequest, err := c.saveRequest()
	if err != nil {
		c.errs.Append(err)
		return
	}

	// Set metadata
	if setMetadataRequest != nil {
		if saveRequest == nil {
			// Set metadata now because there is no change in the object.
			c.poolFor(c.parentCtx.Object.Level()).Request(setMetadataRequest).Send()
		} else {
			// Set metadata if save has been successful.
			saveRequest.OnSuccess(func(response *client.Response) {
				response.WaitFor(setMetadataRequest)
				response.Sender().Send(setMetadataRequest) // use same pool
			})
		}
	}

	// OnSuccess callback
	saveRequest.OnSuccess(func(*client.Response) {
		// Notify UnitOfWork
		c.parentCtx.OnSuccess()
	})
}

func (c *saveContext) saveRequest() (*client.Request, error) {
	if c.parentCtx.ObjectExists {
		return c.updateRequest()
	} else {
		return c.createRequest()
	}
}

func (c *saveContext) createRequest() (*client.Request, error) {
	request, err := c.storageApi.CreateRequest(c.parentCtx.Object)
	if err != nil {
		return nil, err
	}

	// Create request
	return c.poolFor(c.parentCtx.Object.Level()).
		Request(request).
		// Handle if the object already exists
		OnError(func(response *client.Response) {
			if e, ok := response.Error().(*storageapi.Error); ok {
				if e.ErrCode == "configurationAlreadyExists" || e.ErrCode == "configurationRowAlreadyExists" {
					// Object exists -> update instead of create + clear error
					if updateRequest, err := c.updateRequest(); err != nil {
						response.SetErr(err)
					} else {
						response.SetErr(nil)
						response.WaitFor(updateRequest)
						updateRequest.Send()
					}
				}
			}
		}), nil
}

func (c *saveContext) updateRequest() (*client.Request, error) {
	// Skip if no field has been changed
	if c.parentCtx.ChangedFields.IsEmpty() {
		return nil, nil
	}

	// Create request
	request, err := c.storageApi.UpdateRequest(c.parentCtx.Object, c.parentCtx.ChangedFields)
	if err != nil {
		return nil, err
	}

	return c.poolFor(c.parentCtx.Object.Level()).Request(request), nil
}
