package testproject

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"
	"github.com/sasha-s/go-deadlock"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/reflecthelper"
)

// NewSnapshot - to validate final project state in tests.
func (p *Project) NewSnapshot() (*fixtures.ProjectSnapshot, error) {
	lock := &deadlock.Mutex{}
	snapshot := &fixtures.ProjectSnapshot{}
	configsMap := make(map[keboola.ConfigKey]*fixtures.Config)
	configsMetadataMap := make(map[keboola.ConfigKey]keboola.Metadata)

	ctx, cancelFn := context.WithCancelCause(p.ctx)
	grp, ctx := errgroup.WithContext(ctx)
	defer cancelFn(errors.New("snapshot creation cancelled"))

	// Branches
	grp.Go(func() error {
		req := p.keboolaProjectAPI.
			ListBranchesRequest().
			WithOnSuccess(func(ctx context.Context, apiBranches *[]*keboola.Branch) error {
				wg := request.NewWaitGroup(ctx)
				for _, apiBranch := range *apiBranches {
					branch := &fixtures.BranchWithConfigs{Branch: &fixtures.Branch{}, Configs: make([]*fixtures.Config, 0)}
					branch.Name = apiBranch.Name
					branch.Description = apiBranch.Description
					branch.IsDefault = apiBranch.IsDefault
					branch.Metadata = make(map[string]string)
					snapshot.Branches = append(snapshot.Branches, branch)

					// Load branch metadata
					wg.Send(p.keboolaProjectAPI.
						ListBranchMetadataRequest(apiBranch.BranchKey).
						WithOnSuccess(func(_ context.Context, metadata *keboola.MetadataDetails) error {
							branch.Metadata = metadata.ToMap()
							return nil
						}),
					)

					// Load configs and rows
					wg.Send(p.keboolaProjectAPI.
						ListConfigsAndRowsFrom(apiBranch.BranchKey).
						WithOnSuccess(func(ctx context.Context, components *[]*keboola.ComponentWithConfigs) error {
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
									if component.ID != keboola.WorkspacesComponent {
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
					wg.Send(p.keboolaProjectAPI.
						ListConfigMetadataRequest(apiBranch.ID).
						WithOnSuccess(func(_ context.Context, metadata *keboola.ConfigsMetadata) error {
							for _, item := range *metadata {
								configKey := keboola.ConfigKey{BranchID: item.BranchID, ComponentID: item.ComponentID, ID: item.ConfigID}
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
						config.Metadata = make(keboola.Metadata)
					}
				}

				return nil
			})
		return req.SendOrErr(ctx)
	})

	// Schedules for main branch
	var schedules []*keboola.Schedule
	grp.Go(func() error {
		req := p.keboolaProjectAPI.
			ListSchedulesRequest().
			WithOnSuccess(func(_ context.Context, apiSchedules *[]*keboola.Schedule) error {
				schedules = append(schedules, *apiSchedules...)
				return nil
			})
		return req.SendOrErr(ctx)
	})

	workspacesMap := make(map[string]*keboola.Workspace)
	grp.Go(func() error {
		req := p.keboolaProjectAPI.
			ListWorkspaceInstancesRequest().
			WithOnSuccess(func(ctx context.Context, result *[]*keboola.Workspace) error {
				for _, sandbox := range *result {
					workspacesMap[sandbox.ID.String()] = sandbox
				}
				return nil
			})
		return req.SendOrErr(ctx)
	})

	// Storage Buckets
	bucketsMap := map[keboola.BucketID]*fixtures.Bucket{}
	grp.Go(func() error {
		req := p.keboolaProjectAPI.
			ListBucketsRequest(p.defaultBranch.ID).
			WithOnSuccess(func(_ context.Context, apiBuckets *[]*keboola.Bucket) error {
				for _, b := range *apiBuckets {
					bucketsMap[b.BucketID] = &fixtures.Bucket{
						ID:          b.BucketID,
						URI:         b.URI,
						DisplayName: b.DisplayName,
						Description: b.Description,
						Tables:      make([]*fixtures.Table, 0),
					}
				}
				return nil
			})
		return req.SendOrErr(ctx)
	})

	// Storage Tables
	var tables []*keboola.Table
	grp.Go(func() error {
		req := p.keboolaProjectAPI.
			ListTablesRequest(p.defaultBranch.ID).
			WithOnSuccess(func(ctx context.Context, apiTables *[]*keboola.Table) error {
				for _, table := range *apiTables {
					// The table list does not contain the "definition" field, so we have to load the tables separately.
					grp.Go(func() error {
						return p.keboolaProjectAPI.
							GetTableRequest(table.TableKey).
							WithOnSuccess(func(ctx context.Context, table *keboola.Table) error {
								tables = append(tables, table)
								return nil
							}).
							SendOrErr(ctx)
					})
				}
				return nil
			})
		return req.SendOrErr(ctx)
	})

	// Storage Files
	var files []*keboola.File
	grp.Go(func() error {
		// Files metadata are not atomic, wait a moment.
		// The creation/deletion of the file does not take effect immediately.
		// We accept the result if it is 10x the same.
		for attempt := 0; attempt < 10; attempt++ {
			time.Sleep(100 * time.Millisecond)
			err := p.keboolaProjectAPI.
				ListFilesRequest(p.defaultBranch.ID).
				WithOnSuccess(func(_ context.Context, apiFiles *[]*keboola.File) error {
					// Reset the counter, if results differs.
					// Ignore the URL field, it may contain a random/volatile time-based signature.
					if files != nil && !cmp.Equal(&files, apiFiles, cmpopts.IgnoreFields(keboola.File{}, "URL")) {
						attempt = 0
					}
					files = *apiFiles
					return nil
				}).
				SendOrErr(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Wait for requests
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	// Join buckets with tables
	for _, t := range tables {
		b := bucketsMap[t.TableID.BucketID]
		b.Tables = append(b.Tables, &fixtures.Table{
			ID:          t.TableID,
			URI:         t.URI,
			Name:        t.Name,
			DisplayName: t.DisplayName,
			Definition:  t.Definition,
			PrimaryKey:  t.PrimaryKey,
			Columns:     t.Columns,
			RowsCount:   t.RowsCount,
		})
	}

	snapshot.Buckets = make([]*fixtures.Bucket, 0)
	for _, b := range bucketsMap {
		sort.Slice(b.Tables, func(i, j int) bool {
			return b.Tables[i].ID.String() < b.Tables[j].ID.String()
		})
		snapshot.Buckets = append(snapshot.Buckets, b)
	}
	sort.Slice(snapshot.Buckets, func(i, j int) bool {
		return snapshot.Buckets[i].ID.String() < snapshot.Buckets[j].ID.String()
	})

	// Storage Files
	snapshot.Files = make([]*fixtures.File, 0)
	for _, f := range files {
		snapshot.Files = append(snapshot.Files, &fixtures.File{
			Name:        f.Name,
			Tags:        f.Tags,
			IsEncrypted: f.IsEncrypted,
			IsPermanent: f.IsPermanent,
			IsSliced:    f.IsSliced,
		})
	}
	sort.Slice(snapshot.Files, func(i, j int) bool {
		return snapshot.Files[i].Name < snapshot.Files[j].Name
	})

	defBranch, err := p.DefaultBranch()
	if err != nil {
		return nil, err
	}

	// Join schedules with config name
	for _, schedule := range schedules {
		configKey := keboola.ConfigKey{BranchID: defBranch.ID, ComponentID: keboola.SchedulerComponentID, ID: schedule.ConfigID}
		if scheduleConfig, found := configsMap[configKey]; found {
			snapshot.Schedules = append(snapshot.Schedules, &fixtures.Schedule{Name: scheduleConfig.Name})
		} else {
			snapshot.Schedules = append(snapshot.Schedules, &fixtures.Schedule{Name: "SCHEDULE CONFIG NOT FOUND"})
		}
	}

	// Join sandbox instances with config name
	for _, config := range configsMap {
		if config.ComponentID == keboola.WorkspacesComponent {
			sandboxID, err := keboola.GetWorkspaceID(config.ToAPI().Config)
			if err != nil {
				snapshot.Sandboxes = append(snapshot.Sandboxes, &fixtures.Sandbox{Name: "SANDBOX INSTANCE ID NOT SET"})
				continue
			}

			if sandbox, found := workspacesMap[sandboxID.String()]; found {
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
