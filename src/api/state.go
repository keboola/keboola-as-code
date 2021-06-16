package api

import (
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

func LoadState(api *StorageApi) (*model.State, *utils.Error) {
	state := model.NewState()
	pool := api.NewPool()

	// Load branches
	pool.
		Request(api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) *client.Response {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				if ok := state.AddBranch(branch); !ok {
					continue
				}

				// Load components
				pool.
					Request(api.ListComponentsRequest(branch.Id)).
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
