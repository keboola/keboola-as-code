package api

import (
	"fmt"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
)

func LoadRemoteState(api *StorageApi) (*remote.State, error) {
	state := remote.NewState()
	pool := api.NewPool(func(pool *client.Pool, response *client.PoolResponse) error {
		if response.HasError() {
			return response.Error()
		}

		if !response.HasResponse() {
			panic(fmt.Errorf("pool http response is not set"))
		}

		result := response.Response().Result()
		if result == nil {
			panic(fmt.Errorf("pool http result is not set"))
		}

		return processResult(state, api, pool, result)
	})
	pool.Send(api.ListBranchesReq())
	if err := pool.StartAndWait(); err != nil {
		return nil, err
	}
	return state, nil
}

func processResult(state *remote.State, api *StorageApi, pool *client.Pool, result interface{}) error {
	switch value := result.(type) {
	case []*remote.Branch:
		// Save + load components
		for _, b := range value {
			state.AddBranch(b)
			pool.Send(api.ListComponentsReq(b.Id))
		}
	case []*remote.Component:
		// Save - component contains all configs and rows
		for _, c := range value {
			state.AddComponent(c)
		}
	default:
		panic(fmt.Errorf("unexpected pool result type \"%t\"", result))
	}

	return nil
}
