package state

import (
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

// doLoadRemoteState - API -> unified model
func (s *State) doLoadRemoteState() {
	s.remoteErrors = utils.NewMultiError()
	pool := s.api.NewPool()
	pool.SetContext(s.context)

	// Load branches
	pool.
		Request(s.api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				// Skip ignored branches
				if !s.manifest.IsBranchAllowed(branch.Id, branch.Name) {
					continue
				}

				// Save to state
				s.SetRemoteState(branch)

				// Load components
				pool.
					Request(s.api.ListComponentsRequest(branch.Id)).
					OnSuccess(func(response *client.Response) {
						// Save component, it contains all configs and rows
						for _, component := range *response.Result().(*[]*model.ComponentWithConfigs) {
							for _, config := range component.Configs {
								s.SetRemoteState(config.Config)
								for _, row := range config.Rows {
									s.SetRemoteState(row)
								}
							}
						}
					}).
					Send()
			}
		}).
		Send()

	if err := pool.StartAndWait(); err != nil {
		s.AddRemoteError(err)
	}
}
