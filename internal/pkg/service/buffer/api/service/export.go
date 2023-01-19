package service

import (
	"context"
	"reflect"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *service) CreateExport(d dependencies.ForProjectRequest, payload *buffer.CreateExportPayload) (res *buffer.Export, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	rb := rollback.New(s.logger)
	defer rb.InvokeIfErr(ctx, &err)

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
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

	if err := s.createResourcesForExport(ctx, d, rb, &export); err != nil {
		return nil, err
	}

	if err := str.CreateExport(ctx, export); err != nil {
		return nil, err
	}

	return s.GetExport(d, &buffer.GetExportPayload{ReceiverID: export.ReceiverID, ExportID: export.ExportID})
}

func (s *service) UpdateExport(d dependencies.ForProjectRequest, payload *buffer.UpdateExportPayload) (res *buffer.Export, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	rb := rollback.New(s.logger)
	defer rb.InvokeIfErr(ctx, &err)

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: payload.ExportID}
	err = str.UpdateExport(ctx, exportKey, func(export model.Export) (model.Export, error) {
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

	if err != nil {
		return nil, err
	}

	return s.GetExport(d, &buffer.GetExportPayload{ReceiverID: exportKey.ReceiverID, ExportID: exportKey.ExportID})
}

func (s *service) GetExport(d dependencies.ForProjectRequest, payload *buffer.GetExportPayload) (r *buffer.Export, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: payload.ExportID}
	export, err := str.GetExport(ctx, exportKey)
	if err != nil {
		return nil, err
	}

	return s.mapper.ExportPayload(export), nil
}

func (s *service) ListExports(d dependencies.ForProjectRequest, payload *buffer.ListExportsPayload) (r *buffer.ExportsList, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
	exports, err := str.ListExports(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	return &buffer.ExportsList{Exports: s.mapper.ExportsPayload(exports)}, nil
}

func (s *service) DeleteExport(d dependencies.ForProjectRequest, payload *buffer.DeleteExportPayload) (err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: payload.ExportID}
	return str.DeleteExport(ctx, exportKey)
}

func (s *service) createResourcesForExport(ctx context.Context, d dependencies.ForProjectRequest, rb rollback.Builder, export *model.Export) error {
	// Buket is required by token and table
	if err := d.TableManager().EnsureBucketExists(ctx, rb, export.Mapping.TableID.BucketID); err != nil {
		return err
	}

	// The following operations can be performed in parallel
	rb = rb.AddParallel()
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.TableManager().EnsureTableExists(ctx, rb, export); err != nil {
			errs.Append(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.TokenManager().CreateToken(ctx, rb, export); err != nil {
			errs.Append(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.FileManager().CreateFileForExport(ctx, rb, export); err != nil {
			errs.Append(err)
		}
	}()

	wg.Wait()
	return errs.ErrorOrNil()
}
