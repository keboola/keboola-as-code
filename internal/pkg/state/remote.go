package state

import (
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

// doLoadRemoteState - API -> unified model.
func (s *State) doLoadRemoteState() {
	s.remoteErrors = utils.NewMultiError()
	pool := s.api.NewPool()
	pool.SetContext(s.context)

	// Load branches
	pool.
		Request(s.api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			// Save branch + load branch components
			for _, b := range *response.Result().(*[]*model.Branch) {
				branch := b

				// Save to state, skip configs/rows if branch is ignored
				if objectState, err := s.SetRemoteState(branch); err != nil {
					s.AddRemoteError(err)
					continue
				} else if objectState == nil {
					continue
				}

				// Load components
				pool.
					Request(s.api.ListComponentsRequest(branch.Id)).
					OnSuccess(func(response *client.Response) {
						s.processComponents(*response.Result().(*[]*model.ComponentWithConfigs))
					}).
					Send()
			}
		}).
		Send()

	if err := pool.StartAndWait(); err != nil {
		s.AddRemoteError(err)
	}
}

func (s *State) processComponents(components []*model.ComponentWithConfigs) {
	// Save component, it contains all configs and rows
	for _, component := range components {
		// Configs
		for _, config := range component.Configs {
			// Save to state, skip rows if config is ignored
			if objectState, err := s.SetRemoteState(config.Config); err != nil {
				s.AddRemoteError(err)
				continue
			} else if objectState == nil {
				continue
			}

			// Rows
			for _, row := range config.Rows {
				if _, err := s.SetRemoteState(row); err != nil {
					s.AddRemoteError(err)
					continue
				}
			}
		}
	}
}
