package remote

import (
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

type loadContext struct {
	*uow
	state.LoadContext
}

func (c *loadContext) loadAll() {
	// Run all requests in one pool
	pool := c.poolFor(-1)

	// Branches
	pool.
		Request(c.storageApi.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			// Process branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				// Load components
				c.loadBranch(branch, pool)
			}
		}).
		Send()
}

func (c *loadContext) loadBranch(branch *model.Branch, pool *client.Pool) {
	// Add branch to the objects collection
	if !c.process(branch) {
		// Ignored object -> skip children
		return
	}

	// Load metadata for configurations
	metadataMap, metadataRequest := c.loadConfigsMetadataRequest(branch, pool)

	// Load components, configs and rows
	componentsRequest := pool.
		Request(c.storageApi.ListComponentsRequest(branch.BranchId)).
		OnSuccess(func(response *client.Response) {
			components := *response.Result().(*[]*model.ComponentWithConfigs)

			// Component contains all configs and rows
			for _, component := range components {
				// Configs
				for _, config := range component.Configs {
					// Set config metadata
					metadata, found := metadataMap[config.ConfigKey]
					if !found {
						metadata = make(map[string]string)
					}
					config.Metadata = metadata

					// Add config to the objects collection
					if !c.process(config.Config) {
						// Ignored object -> skip children
						continue
					}

					// Rows
					for _, row := range config.Rows {
						c.process(row)
					}
				}
			}
		})

	// Process response after the metadata is loaded
	componentsRequest.WaitFor(metadataRequest)

	// Send requests
	metadataRequest.Send()
	componentsRequest.Send()
}

func (c *loadContext) loadConfigsMetadataRequest(branch *model.Branch, pool *client.Pool) (map[model.Key]map[string]string, *client.Request) {
	lock := &sync.Mutex{}
	out := make(map[model.Key]map[string]string)

	request := pool.
		Request(c.storageApi.ListConfigMetadataRequest(branch.BranchId)).
		OnSuccess(func(response *client.Response) {
			lock.Lock()
			defer lock.Unlock()
			metadataResponse := *response.Result().(*storageapi.ConfigMetadataResponse)
			for key, metadata := range metadataResponse.MetadataMap(branch.BranchId) {
				metadataMap := make(map[string]string)
				for _, m := range metadata {
					metadataMap[m.Key] = m.Value
				}
				out[key] = metadataMap
			}
		})
	return out, request
}

func (c *loadContext) process(apiObject model.Object) (accepted bool) {
	if c.filter.IsObjectIgnored(apiObject) {
		return false
	}

	// Clone object and create recipe
	// During mapping is the API object modified, so it is needed to clone it first.
	object := deepcopy.Copy(apiObject).(model.Object)
	recipe := model.NewRemoteLoadRecipe(object)

	// Invoke mapper
	if err := c.mapper.MapAfterRemoteLoad(recipe); err != nil {
		c.errs.Append(err)
	}

	// Notify UnitOfWork
	return c.OnLoad(apiObject)
}
