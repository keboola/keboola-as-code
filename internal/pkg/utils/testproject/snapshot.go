package testproject

import (
	"context"
	"strings"
	"sync"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/reflecthelper"
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
					branch := &fixtures.BranchWithConfigs{Branch: &fixtures.Branch{}, Configs: make([]*fixtures.Config, 0)}
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

									// Do not snapshot configs which are later joined into a different resource.
									// The component must still exist in `configsMap` so it can be joined later.
									if component.ID != sandboxesapi.Component {
										branch.Configs = append(branch.Configs, config)
									}

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
		return request.SendOrErr(ctx, p.storageAPIClient)
	})

	// Schedules for main branch
	var schedules []*schedulerapi.Schedule
	grp.Go(func() error {
		request := schedulerapi.
			ListSchedulesRequest().
			WithOnSuccess(func(_ context.Context, _ client.Sender, apiSchedules *[]*schedulerapi.Schedule) error {
				schedules = append(schedules, *apiSchedules...)
				return nil
			})
		return request.SendOrErr(ctx, p.schedulerAPIClient)
	})

	sandboxesMap := make(map[string]*sandboxesapi.Sandbox)
	grp.Go(func() error {
		request := sandboxesapi.
			ListInstancesRequest().
			WithOnSuccess(func(ctx context.Context, sender client.Sender, result *[]*sandboxesapi.Sandbox) error {
				for _, sandbox := range *result {
					sandboxesMap[sandbox.ID.String()] = sandbox
				}
				return nil
			})
		return request.SendOrErr(ctx, p.sandboxesAPIClient)
	})

	// Storage Buckets
	bucketsMap := map[storageapi.BucketID]*fixtures.Bucket{}
	grp.Go(func() error {
		request := storageapi.
			ListBucketsRequest().
			WithOnSuccess(func(_ context.Context, _ client.Sender, apiBuckets *[]*storageapi.Bucket) error {
				for _, b := range *apiBuckets {
					bucketsMap[b.ID] = &fixtures.Bucket{
						ID:          b.ID,
						URI:         b.Uri,
						Name:        b.Name,
						DisplayName: b.DisplayName,
						Stage:       b.Stage,
						Description: b.Description,
						Tables:      make([]*fixtures.Table, 0),
					}
				}
				return nil
			})
		return request.SendOrErr(ctx, p.storageAPIClient)
	})

	// Storage Tables
	var tables []*storageapi.Table
	grp.Go(func() error {
		request := storageapi.
			ListTablesRequest().
			WithOnSuccess(func(_ context.Context, _ client.Sender, apiTables *[]*storageapi.Table) error {
				tables = append(tables, *apiTables...)
				return nil
			})
		return request.SendOrErr(ctx, p.storageAPIClient)
	})

	// Wait for requests
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	// Join buckets with tables
	for _, t := range tables {
		b := bucketsMap[t.Bucket.ID]
		b.Tables = append(b.Tables, &fixtures.Table{
			ID:          t.ID,
			URI:         t.Uri,
			Name:        t.Name,
			DisplayName: t.DisplayName,
			PrimaryKey:  t.PrimaryKey,
			Columns:     t.Columns,
		})
	}

	snapshot.Buckets = make([]*fixtures.Bucket, 0)
	for _, b := range bucketsMap {
		snapshot.Buckets = append(snapshot.Buckets, b)
	}

	defBranch, err := p.DefaultBranch()
	if err != nil {
		return nil, err
	}

	// Join schedules with config name
	for _, schedule := range schedules {
		configKey := storageapi.ConfigKey{BranchID: defBranch.ID, ComponentID: storageapi.SchedulerComponentID, ID: schedule.ConfigID}
		if scheduleConfig, found := configsMap[configKey]; found {
			snapshot.Schedules = append(snapshot.Schedules, &fixtures.Schedule{Name: scheduleConfig.Name})
		} else {
			snapshot.Schedules = append(snapshot.Schedules, &fixtures.Schedule{Name: "SCHEDULE CONFIG NOT FOUND"})
		}
	}

	// Join sandbox instances with config name
	for _, config := range configsMap {
		if config.ComponentID == sandboxesapi.Component {
			sandboxID, err := sandboxesapi.GetSandboxID(config.ToAPI().Config)
			if err != nil {
				snapshot.Sandboxes = append(snapshot.Sandboxes, &fixtures.Sandbox{Name: "SANDBOX INSTANCE ID NOT SET"})
				continue
			}

			if sandbox, found := sandboxesMap[sandboxID.String()]; found {
				snapshot.Sandboxes = append(snapshot.Sandboxes, &fixtures.Sandbox{Name: config.Name, Type: sandbox.Type, Size: sandbox.Size})
			} else {
				snapshot.Sandboxes = append(snapshot.Sandboxes, &fixtures.Sandbox{Name: "SANDBOX INSTANCE NOT FOUND"})
			}
		}
	}

	// Sort by name
	reflecthelper.SortByName(snapshot.Branches)
	for _, b := range snapshot.Branches {
		reflecthelper.SortByName(b.Configs)
		for _, c := range b.Configs {
			reflecthelper.SortByName(c.Rows)
		}
	}
	reflecthelper.SortByName(snapshot.Sandboxes)

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
