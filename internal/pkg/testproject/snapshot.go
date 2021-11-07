package testproject

import (
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// NewSnapshot - to validate final project state in tests.
func (p *Project) NewSnapshot() (*fixtures.ProjectSnapshot, error) {
	snapshot := &fixtures.ProjectSnapshot{}
	configs := make(map[string]*fixtures.Config)
	var schedules []*model.Schedule

	// Load Storage API object and Schedules in parallel
	grp := &errgroup.Group{}
	grp.Go(func() error {
		return p.snapshot(snapshot, configs)
	})
	grp.Go(func() (err error) {
		schedules, err = p.SchedulerApi().ListSchedules()
		return err
	})
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	// Map schedules
	for _, schedule := range schedules {
		configKey := model.ConfigKey{BranchId: p.DefaultBranch().Id, ComponentId: model.SchedulerComponentId, Id: schedule.ConfigurationId}
		scheduleConfig := configs[configKey.String()]
		snapshot.Schedules = append(snapshot.Schedules, &fixtures.Schedule{Name: scheduleConfig.Name})
	}

	// Sort by name
	utils.SortByName(snapshot.Branches)
	for _, b := range snapshot.Branches {
		utils.SortByName(b.Configs)
		for _, c := range b.Configs {
			utils.SortByName(c.Rows)
		}
	}

	return snapshot, nil
}

func (p *Project) snapshot(snapshot *fixtures.ProjectSnapshot, configs map[string]*fixtures.Config) error {
	lock := &sync.Mutex{}

	// Load objects from Storage API
	// Branches
	pool := p.StorageApi().NewPool()
	pool.
		Request(p.StorageApi().ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			apiBranches := *response.Result().(*[]*model.Branch)
			for _, branch := range apiBranches {
				b := &fixtures.Branch{}
				b.Name = branch.Name
				b.Description = branch.Description
				b.IsDefault = branch.IsDefault
				branchWithConfigs := &fixtures.BranchWithConfigs{Branch: b, Configs: make([]*fixtures.Config, 0)}
				snapshot.Branches = append(snapshot.Branches, branchWithConfigs)

				// Configs
				pool.
					Request(p.StorageApi().ListComponentsRequest(branch.Id)).
					OnSuccess(func(response *client.Response) {
						apiComponents := *response.Result().(*[]*model.ComponentWithConfigs)
						for _, component := range apiComponents {
							for _, config := range component.Configs {
								c := &fixtures.Config{Rows: make([]*fixtures.ConfigRow, 0)}
								c.ComponentId = config.ComponentId
								c.Name = config.Name
								c.Description = config.Description
								c.ChangeDescription = normalizeChangeDesc(config.ChangeDescription)
								c.Content = config.Content
								branchWithConfigs.Configs = append(branchWithConfigs.Configs, c)

								lock.Lock()
								configs[config.Key().String()] = c
								lock.Unlock()

								// Rows
								for _, row := range config.Rows {
									r := &fixtures.ConfigRow{}
									r.Name = row.Name
									r.Description = row.Description
									r.ChangeDescription = normalizeChangeDesc(row.ChangeDescription)
									r.IsDisabled = row.IsDisabled
									r.Content = row.Content
									c.Rows = append(c.Rows, r)
								}
							}
						}
					}).
					Send()
			}
		}).
		Send()

	// Wait for requests
	return pool.StartAndWait()
}

func normalizeChangeDesc(str string) string {
	// Default description if object has been created by test
	if str == "created by test" {
		return ""
	}

	// Default description if object has been created with a new branch
	if strings.HasPrefix(str, "Copied from ") {
		return ""
	}
	// Default description if rows has been deleted
	if strings.HasSuffix(str, " deleted") {
		return ""
	}

	return str
}
