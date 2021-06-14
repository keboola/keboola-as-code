package api

import (
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
)

func LoadRemoteState(api *StorageApi) (*remote.State, error) {
	state := remote.NewState()
	pool := api.NewPool()
	pool.Request(api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) *client.Response {
			// Save branch + load branch components
			for _, b := range response.Result().([]*remote.Branch) {
				state.AddBranch(b)
				pool.Request(api.ListComponentsRequest(b.Id)).
					OnSuccess(func(response *client.Response) *client.Response {
						// Save component, it contains all configs and rows
						for _, c := range response.Result().([]*remote.Component) {
							state.AddComponent(c)
						}
						return response
					})
			}
			return response
		})

	if err := pool.StartAndWait(); err != nil {
		return nil, err
	}
	return state, nil
}
