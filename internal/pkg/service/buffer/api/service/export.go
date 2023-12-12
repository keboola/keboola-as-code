package service

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	exportCreateTaskType = "export.create"
	exportUpdateTaskType = "export.update"
)

func (s *service) CreateExport(ctx context.Context, d dependencies.ProjectRequestScope, payload *buffer.CreateExportPayload) (res *buffer.Task, err error) {
	str := d.Store()

	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: payload.ReceiverID}
	export, err := s.mapper.CreateExportModel(
		receiverKey,
		buffer.CreateExportData{
			ID:         payload.ID,
			Name:       payload.Name,
			Mapping:    payload.Mapping,
			Conditions: payload.Conditions,
		},
	)
	if err != nil {
		return nil, err
	}

	// Check if export does not exist and the exports count limit is not reached.
	err = str.CheckCreateExport(ctx, export.ExportKey)
	if err != nil {
		return nil, err
	}

	t, err := d.TaskNode().StartTask(task.Config{
		Type: exportCreateTaskType,
		Key: task.Key{
			ProjectID: receiverKey.ProjectID,
			TaskID: task.ID(strings.Join([]string{
				export.ReceiverID.String(),
				export.ExportID.String(),
				exportCreateTaskType,
			}, "/")),
		},
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			err := func() (err error) {
				rb := rollback.New(s.logger)
				defer rb.InvokeIfErr(ctx, &err)

				if err := s.createResourcesForExport(ctx, d, rb, &export); err != nil {
					return err
				}

				if err := str.CreateExport(ctx, export); err != nil {
					return err
				}

				return nil
			}()
			if err != nil {
				return task.ErrResult(err)
			}

			return task.
				OkResult("export created").
				WithOutput("receiverId", export.ReceiverID.String()).
				WithOutput("exportId", export.ExportID.String())
		},
	})
	if err != nil {
		return nil, err
	}
	return s.mapper.TaskPayload(t), nil
}

func (s *service) UpdateExport(ctx context.Context, d dependencies.ProjectRequestScope, payload *buffer.UpdateExportPayload) (res *buffer.Task, err error) {
	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: payload.ReceiverID}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: payload.ExportID}

	t, err := d.TaskNode().StartTask(task.Config{
		Type: exportUpdateTaskType,
		Key: task.Key{
			ProjectID: receiverKey.ProjectID,
			TaskID: task.ID(strings.Join([]string{
				exportKey.ReceiverID.String(),
				exportKey.ExportID.String(),
				exportUpdateTaskType,
			}, "/")),
		},
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			err := func() (err error) {
				rb := rollback.New(s.logger)
				defer rb.InvokeIfErr(ctx, &err)

				return d.Store().UpdateExport(ctx, exportKey, func(export model.Export) (model.Export, error) {
					oldMapping := export.Mapping
					if err := s.mapper.UpdateExportModel(&export, payload); err != nil {
						return export, err
					}

					// Create resources for the modified mapping
					if !reflect.DeepEqual(oldMapping, export.Mapping) {
						if err := s.createResourcesForExport(ctx, d, rb, &export); err != nil {
							return export, err
						}
					}

					return export, nil
				})
			}()
			if err != nil {
				return task.ErrResult(err)
			}

			return task.
				OkResult("export updated").
				WithOutput("receiverId", exportKey.ReceiverID.String()).
				WithOutput("exportId", exportKey.ExportID.String())
		},
	})
	if err != nil {
		return nil, err
	}
	return s.mapper.TaskPayload(t), nil
}

func (s *service) GetExport(ctx context.Context, d dependencies.ProjectRequestScope, payload *buffer.GetExportPayload) (r *buffer.Export, err error) {
	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: payload.ReceiverID}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: payload.ExportID}
	export, err := d.Store().GetExport(ctx, exportKey)
	if err != nil {
		return nil, err
	}

	return s.mapper.ExportPayload(export), nil
}

func (s *service) ListExports(ctx context.Context, d dependencies.ProjectRequestScope, payload *buffer.ListExportsPayload) (r *buffer.ExportsList, err error) {
	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: payload.ReceiverID}
	exports, err := d.Store().ListExports(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	return &buffer.ExportsList{Exports: s.mapper.ExportsPayload(exports)}, nil
}

func (s *service) DeleteExport(ctx context.Context, d dependencies.ProjectRequestScope, payload *buffer.DeleteExportPayload) (err error) {
	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: payload.ReceiverID}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: payload.ExportID}
	return d.Store().DeleteExport(ctx, exportKey)
}

func (s *service) createResourcesForExport(ctx context.Context, d dependencies.ProjectRequestScope, rb rollback.Builder, export *model.Export) error {
	// Bucket is required by token and table
	if err := d.TableManager().EnsureBucketExists(ctx, rb, export.Mapping.TableID.BucketID); err != nil {
		return err
	}

	// Create token
	if err := d.TokenManager().CreateToken(ctx, rb, export); err != nil {
		return err
	}

	// The following operations can be performed in parallel
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}
	rb = rb.AddParallel()

	// Create table
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.TableManager().EnsureTableExists(ctx, rb, export); err != nil {
			errs.Append(err)
		}
	}()

	// Create file
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.FileManager().WithToken(export.Token.Token).CreateFileForExport(ctx, rb, export); err != nil {
			errs.Append(err)
		}
	}()

	wg.Wait()
	return errs.ErrorOrNil()
}
