package state

import (
	"context"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
)

// LoadRemoteState - API -> unified model
func (s *State) LoadRemoteState(ctx context.Context) {
	pool := s.api.NewPool()

	// Load branches
	pool.
		Request(s.api.ListBranchesRequest()).
		SetContext(ctx).
		OnSuccess(func(response *client.Response) *client.Response {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				s.SetBranchRemoteState(branch)

				// Load components
				pool.
					Request(s.api.ListComponentsRequest(branch.Id)).
					SetContext(ctx).
					OnSuccess(func(response *client.Response) *client.Response {
						// Save component, it contains all configs and rows
						for _, component := range *response.Result().(*[]*model.ComponentWithConfigs) {
							for _, config := range component.Configs {
								s.SetConfigRemoteState(component.Component, config.Config)
								for _, row := range config.Rows {
									s.SetConfigRowRemoteState(row)
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
		s.AddRemoteError(err)
	}
}
