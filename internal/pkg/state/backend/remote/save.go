package remote

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type saveContext struct {
	*uow
	state.SaveContext
}

func (c *saveContext) save() {
	// Invoke mapper
	recipe := model.NewRemoteSaveRecipe(c.Object, c.ChangedFields)
	if err := c.mapper.MapBeforeRemoteSave(recipe); err != nil {
		c.errs.Append(err)
		return
	}

	// Relations are stored on the API side in config/row configuration.
	// This is ensured by the mapper layer.
	if recipe.ChangedFields.Has(`relations`) {
		recipe.ChangedFields.Add(`configuration`)
		recipe.ChangedFields.Remove(`relations`)
	}

	// Branch cannot be created, it must be cloned
	if v, ok := c.Object.(*model.Branch); ok && !c.ObjectExists {
		c.errs.Append(fmt.Errorf(`branch "%d" (%s) cannot be created, it must be created as clone of the main branch directly in the project`, v.BranchId, v.Name))
		return
	}

	// Set changeDescription
	switch v := c.Object.(type) {
	case *model.Config:
		v.ChangeDescription = c.changeDescription
		c.ChangedFields.Add("changeDescription")
	case *model.ConfigRow:
		v.ChangeDescription = c.changeDescription
		c.ChangedFields.Add("changeDescription")
	}

	// Should metadata be set?
	setMetadata := !c.ObjectExists || c.ChangedFields.Has("metadata")
	var setMetadataRequest *client.Request
	if setMetadata {
		c.ChangedFields.Remove("metadata")
		setMetadataRequest = c.storageApi.AppendMetadataRequest(c.Object)
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
			c.poolFor(c.Object.Level()).Request(setMetadataRequest).Send()
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
		c.OnSuccess()
	})
}

func (c *saveContext) saveRequest() (*client.Request, error) {
	if c.ObjectExists {
		return c.updateRequest()
	} else {
		return c.createRequest()
	}
}

func (c *saveContext) createRequest() (*client.Request, error) {
	request, err := c.storageApi.CreateRequest(c.Object)
	if err != nil {
		return nil, err
	}

	// Create request
	return c.poolFor(c.Object.Level()).
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
	if c.ChangedFields.IsEmpty() {
		return nil, nil
	}

	// Create request
	request, err := c.storageApi.UpdateRequest(c.Object, c.ChangedFields)
	if err != nil {
		return nil, err
	}

	return c.poolFor(c.Object.Level()).Request(request), nil
}
