package api

import (
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
)

func LoadState(api *StorageApi) (*model.State, error) {
	state := model.NewState()
	pool := api.NewPool()

	// Load branches
	pool.
		Request(api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) *client.Response {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				if err := state.AddBranch(branch); err != nil {
					return response.SetError(err)
				}

				// Load components
				pool.
					Request(api.ListComponentsRequest(branch.Id)).
					OnSuccess(func(response *client.Response) *client.Response {
						// Save component, it contains all configs and rows
						for _, component := range *response.Result().(*[]*model.Component) {
							if err := state.AddComponent(component); err != nil {
								return response.SetError(err)
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
		return nil, err
	}
	return state, nil
}
