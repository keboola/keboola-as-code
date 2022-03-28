package remote

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

type saveCtx struct {
	*uow
	object        model.Object
	recipe        *model.RemoteSaveRecipe
	changedFields model.ChangedFields
	objectExists  bool
	onSuccess     func()
}

func (c *saveCtx) save() {
	// Clone object and create recipe
	// During mapping is the internal object modified, so it is needed to clone it first.
	apiObject := deepcopy.Copy(c.object).(model.Object)
	c.recipe = model.NewRemoteSaveRecipe(apiObject, c.changedFields)

	// Invoke mapper
	if err := c.mapper.MapBeforeRemoteSave(c.recipe); err != nil {
		c.errors.Append(err)
		return
	}

	// Branch cannot be created, it must be cloned
	if v, ok := c.recipe.Object.(*model.Branch); ok && !c.objectExists {
		c.errors.Append(fmt.Errorf(`branch "%d" (%s) cannot be created, it must be created as clone of the main branch directly in the project`, v.Id, v.Name))
		return
	}

	// Set changeDescription
	switch v := c.recipe.Object.(type) {
	case *model.Config:
		v.ChangeDescription = c.changeDescription
		c.changedFields.Add("changeDescription")
	case *model.ConfigRow:
		v.ChangeDescription = c.changeDescription
		c.changedFields.Add("changeDescription")
	}

	// Should metadata be set?
	setMetadata := !c.objectExists || c.changedFields.Has("metadata")
	var setMetadataRequest *client.Request
	if setMetadata {
		c.changedFields.Remove("metadata")
		setMetadataRequest = c.storageApi.AppendMetadataRequest(c.recipe.Object)
	}

	// Create request
	saveRequest, err := c.saveRequest()
	if err != nil {
		c.errors.Append(err)
		return
	}

	// Set metadata
	if setMetadataRequest != nil {
		if saveRequest == nil {
			// Set metadata now because there is no change in the object.
			c.poolFor(c.recipe.Object.Level()).Request(setMetadataRequest).Send()
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
		c.onSuccess()
	})
}

func (c *saveCtx) saveRequest() (*client.Request, error) {
	if c.objectExists {
		return c.updateRequest()
	} else {
		return c.createRequest()
	}
}

func (c *saveCtx) createRequest() (*client.Request, error) {
	request, err := c.storageApi.CreateRequest(c.recipe.Object)
	if err != nil {
		return nil, err
	}

	// Create request
	return c.poolFor(c.recipe.Object.Level()).
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

func (c *saveCtx) updateRequest() (*client.Request, error) {
	// Skip if no field has been changed
	if c.changedFields.IsEmpty() {
		return nil, nil
	}

	// Create request
	request, err := c.storageApi.UpdateRequest(c.recipe.Object, c.changedFields)
	if err != nil {
		return nil, err
	}

	return c.poolFor(c.recipe.Object.Level()).Request(request), nil
}
