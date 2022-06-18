package testproject

import (
	"context"
	"strings"
	"sync"

	"github.com/keboola/go-client/pkg/schedulerapi"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// NewSnapshot - to validate final project state in tests.
func (p *Project) NewSnapshot() (*fixtures.ProjectSnapshot, error) {
	lock := &sync.Mutex{}
	snapshot := &fixtures.ProjectSnapshot{}
	configsMap := make(map[storageapi.ConfigKey]*fixtures.Config)
	configsMetadataMap := make(map[storageapi.ConfigKey]storageapi.Metadata)

	ctx, cancelFn := context.WithCancel(p.ctx)
	grp, ctx := errgroup.WithContext(ctx)
	defer cancelFn()

	// Branches
	grp.Go(func() error {
		request := storageapi.
			ListBranchesRequest().
			WithOnSuccess(func(ctx context.Context, sender client.Sender, apiBranches *[]*storageapi.Branch) error {
				wg := client.NewWaitGroup(ctx, sender)
				for _, apiBranch := range *apiBranches {
					apiBranch := apiBranch
					branch := &fixtures.BranchWithConfigs{Configs: make([]*fixtures.Config, 0)}
					branch.Name = apiBranch.Name
					branch.Description = apiBranch.Description
					branch.IsDefault = apiBranch.IsDefault
					branch.Metadata = make(map[string]string)
					snapshot.Branches = append(snapshot.Branches, branch)

					// Load branch metadata
					wg.Send(storageapi.
						ListBranchMetadataRequest(apiBranch.BranchKey).
						WithOnSuccess(func(_ context.Context, _ client.Sender, metadata *storageapi.MetadataDetails) error {
							branch.Metadata = metadata.ToMap()
							return nil
						}),
					)

					// Load configs and rows
					wg.Send(storageapi.
						ListConfigsAndRowsFrom(apiBranch.BranchKey).
						WithOnSuccess(func(ctx context.Context, sender client.Sender, components *[]*storageapi.ComponentWithConfigs) error {
							for _, component := range *components {
								for _, apiConfig := range component.Configs {
									config := &fixtures.Config{Rows: make([]*fixtures.ConfigRow, 0)}
									config.ComponentID = apiConfig.ComponentID
									config.Name = apiConfig.Name
									config.Description = apiConfig.Description
									config.ChangeDescription = normalizeChangeDesc(apiConfig.ChangeDescription)
									config.Content = apiConfig.Content
									branch.Configs = append(branch.Configs, config)

									lock.Lock()
									configsMap[apiConfig.ConfigKey] = config
									lock.Unlock()

									for _, apiRow := range apiConfig.Rows {
										row := &fixtures.ConfigRow{}
										row.Name = apiRow.Name
										row.Description = apiRow.Description
										row.ChangeDescription = normalizeChangeDesc(apiRow.ChangeDescription)
										row.IsDisabled = apiRow.IsDisabled
										row.Content = apiRow.Content
										config.Rows = append(config.Rows, row)
									}
								}
							}
							return nil
						}),
					)

					// Load configs metadata
					wg.Send(storageapi.
						ListConfigMetadataRequest(apiBranch.ID).
						WithOnSuccess(func(_ context.Context, _ client.Sender, metadata *storageapi.ConfigsMetadata) error {
							for _, item := range *metadata {
								configKey := storageapi.ConfigKey{BranchID: item.BranchID, ComponentID: item.ComponentID, ID: item.ConfigID}
								value := item.Metadata.ToMap()

								lock.Lock()
								configsMetadataMap[configKey] = value
								lock.Unlock()
							}
							return nil
						}),
					)
				}

				// Wait for sub-requests
				if err := wg.Wait(); err != nil {
					return err
				}

				// Join configs and configs metadata
				for key, config := range configsMap {
					if metadata, found := configsMetadataMap[key]; found {
						config.Metadata = metadata
					} else {
						config.Metadata = make(storageapi.Metadata)
					}
				}

				return nil
			})
		return request.SendOrErr(ctx, p.storageApiClient)
	})

	// Schedules for main branch
	var schedules []*schedulerapi.Schedule
	grp.Go(func() error {
		request := schedulerapi.
			ListSchedulesRequest().
			WithOnSuccess(func(_ context.Context, _ client.Sender, apiSchedules *[]*schedulerapi.Schedule) error {
				for _, apiSchedule := range *apiSchedules {
					schedules = append(schedules, apiSchedule)
				}
				return nil
			})
		return request.SendOrErr(ctx, p.schedulerApiClient)
	})

	// Wait for requests
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	// Join schedules with config name
	for _, schedule := range schedules {
		configKey := storageapi.ConfigKey{BranchID: p.DefaultBranch().ID, ComponentID: storageapi.SchedulerComponentID, ID: schedule.ConfigID}
		if scheduleConfig, found := configsMap[configKey]; found {
			snapshot.Schedules = append(snapshot.Schedules, &fixtures.Schedule{Name: scheduleConfig.Name})
		} else {
			snapshot.Schedules = append(snapshot.Schedules, &fixtures.Schedule{Name: "SCHEDULE CONFIG NOT FOUND"})
		}
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
