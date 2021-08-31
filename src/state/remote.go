package state

import (
	"strings"

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
				if s.SetRemoteState(branch) == nil {
					continue
				}

				// Load components
				pool.
					Request(s.api.ListComponentsRequest(branch.Id)).
					OnSuccess(func(response *client.Response) {
						s.processComponents(branch, *response.Result().(*[]*model.ComponentWithConfigs))
					}).
					Send()
			}
		}).
		Send()

	if err := pool.StartAndWait(); err != nil {
		s.AddRemoteError(err)
	}
}

func (s *State) processComponents(branch *model.Branch, components []*model.ComponentWithConfigs) {
	// Save component, it contains all configs and rows
	for _, component := range components {
		// Configs
		for _, config := range component.Configs {
			// Is config from a dev branch marked to delete?
			if !branch.IsDefault && strings.HasPrefix(config.Name, model.ToDeletePrefix) {
				config.MarkToDelete()
			}

			// Save to state, skip rows if config is ignored
			if s.SetRemoteState(config.Config) == nil {
				continue
			}

			// Rows
			for _, row := range config.Rows {
				// Is row from a dev branch marked to delete? Or parent config?
				if (!branch.IsDefault && strings.HasPrefix(row.Name, model.ToDeletePrefix)) ||
					config.IsMarkedToDelete() {
					row.MarkToDelete()
				}
				s.SetRemoteState(row)
			}
		}
	}
}
