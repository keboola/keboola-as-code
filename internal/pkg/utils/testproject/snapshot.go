package testproject

import (
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
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
		configKey := model.ConfigKey{BranchId: p.DefaultBranch().Id, ComponentId: model.SchedulerComponentId, Id: schedule.ConfigId}
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
	branchesMap := make(map[model.BranchId]*fixtures.Branch)
	configsMap := make(map[model.BranchId]map[model.ConfigKey]*fixtures.Config)
	metadataMap := make(map[model.BranchId]map[model.ConfigKey]*map[string]string)

	// Branches
	pool := p.StorageApi().NewPool()
	pool.
		Request(p.StorageApi().ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			apiBranches := *response.Result().(*[]*model.Branch)
			for _, branch := range apiBranches {
				branch := branch
				b := &fixtures.Branch{}
				b.Name = branch.Name
				b.Description = branch.Description
				b.IsDefault = branch.IsDefault
				branchesMap[branch.Id] = b
				configsMap[branch.Id] = make(map[model.ConfigKey]*fixtures.Config)
				metadataMap[branch.Id] = make(map[model.ConfigKey]*map[string]string)

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
								configsMap[branch.Id][config.ConfigKey] = c

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
				pool.
					Request(p.StorageApi().ListConfigMetadataRequest(branch.Id)).
					OnSuccess(func(response *client.Response) {
						metadataResponse := *response.Result().(*storageapi.ConfigMetadataResponse)
						for key, metadata := range metadataResponse.MetadataMap(branch.Id) {
							if len(metadata) > 0 {
								configMetadataMap := make(map[string]string)
								for _, m := range metadata {
									configMetadataMap[m.Key] = m.Value
								}
								metadataMap[branch.Id][key] = &configMetadataMap
							}
						}
					}).
					Send()
			}
		}).
		Send()

	// Wait for requests
	if err := pool.StartAndWait(); err != nil {
		return err
	}

	// Merge configs with metadata
	for branchId, b := range branchesMap {
		branchWithConfigs := &fixtures.BranchWithConfigs{Branch: b, Configs: make([]*fixtures.Config, 0)}
		for configKey, c := range configsMap[branchId] {
			metadata, ok := metadataMap[branchId][configKey]
			if ok {
				c.Metadata = *metadata
			}
			branchWithConfigs.Configs = append(branchWithConfigs.Configs, c)
		}
		snapshot.Branches = append(snapshot.Branches, branchWithConfigs)
	}

	return nil
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
