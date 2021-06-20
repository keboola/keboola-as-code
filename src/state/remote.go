package state

import (
	"context"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
)

func LoadRemoteState(state *model.State, ctx context.Context, api *remote.StorageApi) *utils.Error {
	pool := api.NewPool()

	// Load branches
	pool.
		Request(api.ListBranchesRequest()).
		SetContext(ctx).
		OnSuccess(func(response *client.Response) *client.Response {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				state.SetBranchRemoteState(branch)

				// Load components
				pool.
					Request(api.ListComponentsRequest(branch.Id)).
					SetContext(ctx).
					OnSuccess(func(response *client.Response) *client.Response {
						// Save component, it contains all configs and rows
						for _, component := range *response.Result().(*[]*model.ComponentWithConfigs) {
							for _, config := range component.Configs {
								state.SetConfigRemoteState(component.Component, config)
								for _, row := range config.Rows {
									state.SetConfigRowRemoteState(row)
								}
							}
						}
						return response
					}).
					Send()
			}
			return response
		}).
		Send()

	if err := pool.StartAndWait(); err != nil {
		state.AddRemoteError(err)
	}

	return state.RemoteErrors()
}
