package api

import (
	"context"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

func (a *StorageApi) LoadRemoteState(ctx context.Context) (*model.State, *utils.Error) {
	state := model.NewState()
	pool := a.NewPool()

	// Load branches
	pool.
		Request(a.ListBranchesRequest()).
		SetContext(ctx).
		OnSuccess(func(response *client.Response) *client.Response {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				if ok := state.AddBranch(branch); !ok {
					continue
				}

				// Load components
				pool.
					Request(a.ListComponentsRequest(branch.Id)).
					SetContext(ctx).
					OnSuccess(func(response *client.Response) *client.Response {
						// Save component, it contains all configs and rows
						for _, component := range *response.Result().(*[]*model.Component) {
							state.AddComponent(component)
						}
						return response
					}).
					Send()
			}
			return response
		}).
		Send()

	if err := pool.StartAndWait(); err != nil {
		state.AddError(err)
	}
	return state, state.Error()
}
