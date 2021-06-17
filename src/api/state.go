package api

import (
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

func LoadRemoteState(a *StorageApi) (*model.State, *utils.Error) {
	state := model.NewState()
	pool := a.NewPool()

	// Load branches
	pool.
		Request(a.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) *client.Response {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				if ok := state.AddBranch(branch); !ok {
					continue
				}

				// Load components
				pool.
					Request(a.ListComponentsRequest(branch.Id)).
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

	if state.Error().Len() > 0 {
		return state, state.Error()
	}
	return state, nil
}
