package api

import (
	"context"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

func (a *StorageApi) LoadRemoteState(state *model.State, ctx context.Context) *utils.Error {
	pool := a.NewPool()

	// Load branches
	pool.
		Request(a.ListBranchesRequest()).
		SetContext(ctx).
		OnSuccess(func(response *client.Response) *client.Response {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				state.SetBranchRemoteState(branch)

				// Load components
				pool.
					Request(a.ListComponentsRequest(branch.Id)).
					SetContext(ctx).
					OnSuccess(func(response *client.Response) *client.Response {
						// Save component, it contains all configs and rows
						for _, component := range *response.Result().(*[]*model.Component) {
							state.SetComponentRemoteState(component)
							for _, config := range component.Configs {
								state.SetConfigRemoteState(config)
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
