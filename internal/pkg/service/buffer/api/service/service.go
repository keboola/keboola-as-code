package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	utilsUrl "github.com/keboola/keboola-as-code/internal/pkg/utils/url"
)

type service struct {
	deps   dependencies.ForServer
	mapper mapper.Mapper
	stats  *statistics.APINode
}

func New(d dependencies.ForServer) buffer.Service {
	return &service{
		deps:   d,
		mapper: mapper.NewMapper(d.BufferAPIHost()),
		stats:  statistics.NewAPINode(d),
	}
}

func (s *service) APIRootIndex(dependencies.ForPublicRequest) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(dependencies.ForPublicRequest) (res *buffer.ServiceDetail, err error) {
	res = &buffer.ServiceDetail{
		API:           "buffer",
		Documentation: "https://buffer.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *service) HealthCheck(dependencies.ForPublicRequest) (res string, err error) {
	return "OK", nil
}

// nolint:godox
// TODO: collect errors instead of bailing on the first one

func (s *service) CreateReceiver(d dependencies.ForProjectRequest, payload *buffer.CreateReceiverPayload) (res *buffer.Receiver, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	// Generate Secret
	secret := idgenerator.ReceiverSecret()

	// Map payload to receiver
	receiver, err := mpr.ReceiverModelFromPayload(key.ProjectID(d.ProjectID()), secret, *payload)
	if err != nil {
		return nil, err
	}

	// Create export tables as necessary and generate tokens
	exports, err := setupExports(ctx, d.StorageAPIClient(), receiver.Exports)
	if err != nil {
		return nil, err
	}
	receiver.Exports = exports

	// Create Storage files for exports
	m := file.NewManager(d.StorageAPIClient())
	files, err := m.CreateFilesForReceiver(ctx, &receiver)
	if err != nil {
		return nil, errors.Errorf("creating Storage files failed: %w", err)
	}

	// Persist receiver
	if err := str.CreateReceiver(ctx, receiver, files); err != nil {
		return nil, err
	}

	return s.GetReceiver(d, &buffer.GetReceiverPayload{ReceiverID: buffer.ReceiverID(receiver.ReceiverID)})
}

func (s *service) UpdateReceiver(d dependencies.ForProjectRequest, payload *buffer.UpdateReceiverPayload) (res *buffer.Receiver, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	// Get export
	receiverKey := key.ReceiverKey{
		ProjectID:  key.ProjectID(d.ProjectID()),
		ReceiverID: key.ReceiverID(payload.ReceiverID),
	}

	old, err := str.GetReceiver(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	// Update
	receiver, err := mpr.UpdateReceiverFromPayload(old, *payload)
	if err != nil {
		return nil, err
	}

	// Persist
	err = str.UpdateReceiver(ctx, receiver)
	if err != nil {
		return nil, err
	}

	receiverData, err := str.GetReceiver(ctx, receiver.ReceiverKey)
	if err != nil {
		return nil, err
	}

	resp := mpr.ReceiverPayloadFromModel(receiverData)

	return &resp, nil
}

func (s *service) GetReceiver(d dependencies.ForProjectRequest, payload *buffer.GetReceiverPayload) (res *buffer.Receiver, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: key.ReceiverID(payload.ReceiverID)}

	receiver, err := str.GetReceiver(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	resp := mpr.ReceiverPayloadFromModel(receiver)

	return &resp, nil
}

func (s *service) ListReceivers(d dependencies.ForProjectRequest, _ *buffer.ListReceiversPayload) (res *buffer.ReceiversList, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	projectID := key.ProjectID(d.ProjectID())
	model, err := str.ListReceivers(ctx, projectID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list receivers in project \"%d\"", projectID)
	}

	receivers := make([]*buffer.Receiver, 0, len(model))
	for _, data := range model {
		receiver := mpr.ReceiverPayloadFromModel(data)
		receivers = append(receivers, &receiver)
	}

	return &buffer.ReceiversList{Receivers: receivers}, nil
}

func (s *service) DeleteReceiver(d dependencies.ForProjectRequest, payload *buffer.DeleteReceiverPayload) (err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: key.ReceiverID(payload.ReceiverID)}
	if err := str.DeleteReceiver(ctx, receiverKey); err != nil {
		return err
	}

	return nil
}

func (s *service) RefreshReceiverTokens(d dependencies.ForProjectRequest, payload *buffer.RefreshReceiverTokensPayload) (res *buffer.Receiver, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: key.ReceiverID(payload.ReceiverID)}
	tokens, err := str.ListTokens(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	// Refresh tokens in parallel
	refreshedTokens := make([]model.TokenForExport, len(tokens))
	wg := client.NewWaitGroup(ctx, d.StorageAPIClient())
	for i, token := range tokens {
		i := i
		key := token.ExportKey
		wg.Send(
			storageapi.RefreshTokenRequest(token.Token.ID).
				WithOnSuccess(func(ctx context.Context, sender client.Sender, result *storageapi.Token) error {
					refreshedTokens[i] = model.TokenForExport{
						ExportKey: key,
						Token:     *result,
					}
					return nil
				}),
		)
	}
	if err = wg.Wait(); err != nil {
		return nil, err
	}

	err = str.UpdateTokens(ctx, refreshedTokens)
	if err != nil {
		return nil, err
	}

	return s.GetReceiver(d, &buffer.GetReceiverPayload{ReceiverID: payload.ReceiverID})
}

func (s *service) CreateExport(d dependencies.ForProjectRequest, payload *buffer.CreateExportPayload) (res *buffer.Export, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	// Create Storage file
	m := file.NewManager(d.StorageAPIClient())
	fileRes, err := m.CreateFile(ctx, payload.Name)
	if err != nil {
		return nil, errors.Errorf("creating Storage file failed: %w", err)
	}

	// Map payload to export
	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: key.ReceiverID(payload.ReceiverID)}
	export, err := mpr.ExportModelFromPayload(
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

	// Create export table as necessary and generate token
	export, err = setupSingleExport(ctx, d.StorageAPIClient(), export)
	if err != nil {
		return nil, err
	}

	// Persist export
	if err := str.CreateExport(ctx, export, fileRes); err != nil {
		return nil, err
	}

	return s.GetExport(d, &buffer.GetExportPayload{
		ReceiverID: buffer.ReceiverID(export.ReceiverID),
		ExportID:   buffer.ExportID(export.ExportID),
	})
}

func (s *service) UpdateExport(d dependencies.ForProjectRequest, payload *buffer.UpdateExportPayload) (res *buffer.Export, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	// Get export
	exportKey := key.ExportKey{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  key.ProjectID(d.ProjectID()),
			ReceiverID: key.ReceiverID(payload.ReceiverID),
		},
		ExportID: key.ExportID(payload.ExportID),
	}

	old, err := str.GetExport(ctx, exportKey)
	if err != nil {
		return nil, err
	}

	// Update
	export, err := mpr.UpdateExportFromPayload(old, *payload)
	if err != nil {
		return nil, err
	}

	// If mapping changed, re-do export setup
	if !reflect.DeepEqual(old.Mapping, export.Mapping) {
		// Create export table as necessary and generate token
		export, err = setupSingleExport(ctx, d.StorageAPIClient(), export)
		if err != nil {
			return nil, err
		}
	}

	// Persist
	err = str.UpdateExport(ctx, export)
	if err != nil {
		return nil, err
	}

	return s.GetExport(d, &buffer.GetExportPayload{
		ReceiverID: buffer.ReceiverID(export.ReceiverID),
		ExportID:   buffer.ExportID(export.ExportID),
	})
}

func (s *service) GetExport(d dependencies.ForProjectRequest, payload *buffer.GetExportPayload) (r *buffer.Export, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	exportKey := key.ExportKey{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  key.ProjectID(d.ProjectID()),
			ReceiverID: key.ReceiverID(payload.ReceiverID),
		},
		ExportID: key.ExportID(payload.ExportID),
	}

	export, err := str.GetExport(ctx, exportKey)
	if err != nil {
		return nil, err
	}

	resp := mpr.ExportPayloadFromModel(export)

	return &resp, nil
}

func (s *service) ListExports(d dependencies.ForProjectRequest, payload *buffer.ListExportsPayload) (r *buffer.ExportsList, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	receiverKey := key.ReceiverKey{
		ProjectID:  key.ProjectID(d.ProjectID()),
		ReceiverID: key.ReceiverID(payload.ReceiverID),
	}

	model, err := str.ListExports(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	exports := make([]*buffer.Export, 0, len(model))
	for _, data := range model {
		export := mpr.ExportPayloadFromModel(data)
		exports = append(exports, &export)
	}

	return &buffer.ExportsList{Exports: exports}, nil
}

func (s *service) DeleteExport(d dependencies.ForProjectRequest, payload *buffer.DeleteExportPayload) (err error) {
	ctx, str := d.RequestCtx(), d.Store()

	exportKey := key.ExportKey{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  key.ProjectID(d.ProjectID()),
			ReceiverID: key.ReceiverID(payload.ReceiverID),
		},
		ExportID: key.ExportID(payload.ExportID),
	}
	if err := str.DeleteExport(ctx, exportKey); err != nil {
		return err
	}

	return nil
}

func (s *service) Import(d dependencies.ForPublicRequest, payload *buffer.ImportPayload, reader io.ReadCloser) (err error) {
	ctx, str, header, ip, stats := d.RequestCtx(), d.Store(), d.RequestHeader(), d.RequestClientIP(), s.stats

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(payload.ProjectID), ReceiverID: key.ReceiverID(payload.ReceiverID)}
	receiver, err := str.GetReceiver(ctx, receiverKey)
	if err != nil {
		return err
	}
	if receiver.Secret != payload.Secret {
		return &buffer.GenericError{
			StatusCode: 404,
			Name:       "buffer.receiverNotFound",
			Message:    fmt.Sprintf(`Receiver "%s" with given secret not found.`, payload.ReceiverID),
		}
	}

	data, err := parseRequestBody(payload.ContentType, reader)
	if err != nil {
		return err
	}

	importCtx := column.NewImportCtx(data, header, ip)
	receivedAt := time.Now()

	errs := errors.NewMultiError()
	for _, e := range receiver.Exports {
		// nolint:godox
		// TODO get sliceID and fileID + use in stats.Notify
		fileKey := key.FileKey{ExportKey: e.ExportKey, FileID: key.FileID(receivedAt)}
		sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(receivedAt)}

		csv := make([]string, 0)
		for _, c := range e.Mapping.Columns {
			csvValue, err := c.CsvValue(importCtx)
			if err != nil {
				return err
			}
			csv = append(csv, csvValue)
		}

		record := key.NewRecordKey(sliceKey, receivedAt)
		err = str.CreateRecord(ctx, record, csv)
		if err != nil {
			errs.AppendWithPrefixf(err, `failed to create record for export "%s"`, e.ExportID)
		}

		size := uint64(0)
		for _, column := range csv {
			size += uint64(len(column))
		}
		stats.Notify(sliceKey, size)
	}

	return errs.ErrorOrNil()
}

func parseRequestBody(contentType string, reader io.ReadCloser) (res *orderedmap.OrderedMap, err error) {
	if !isContentTypeForm(contentType) && !regexp.MustCompile(`^application/([a-zA-Z0-9\.\-]+\+)?json$`).MatchString(contentType) {
		return nil, NewUnsupportedMediaTypeError(errors.New(
			"Supported media types are application/json and application/x-www-form-urlencoded.",
		))
	}
	// Limit read csv to 1 MB plus 1 B. If the reader fills the limit then the request is bigger than allowed.
	limit := store.MaxImportRequestSizeInBytes
	limitedReader := io.LimitReader(reader, int64(limit)+1)
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = errors.Errorf("cannot close request body reading: %w", closeErr)
		}
	}()

	buf := new(strings.Builder)
	_, err = io.Copy(buf, limitedReader)
	if err != nil {
		return nil, err
	}

	// Check that the reader did not read more than the maximum.
	if datasize.ByteSize(buf.Len()) > limit {
		return nil, NewPayloadTooLargeError(errors.Errorf("Payload too large, the maximum size is %s.", limit.String()))
	}

	var data *orderedmap.OrderedMap
	if isContentTypeForm(contentType) {
		data, err = utilsUrl.ParseQuery(buf.String())
		if err != nil {
			return nil, NewBadRequestError(errors.New("Could not parse form request body."))
		}
	} else {
		err = json.Unmarshal([]byte(buf.String()), &data)
		if err != nil {
			return nil, NewBadRequestError(errors.New("Could not parse json request body."))
		}
	}
	if data.Len() == 0 {
		return nil, NewBadRequestError(errors.New("Empty request body."))
	}
	return data, nil
}

func isContentTypeForm(t string) bool {
	return strings.HasPrefix(t, "application/x-www-form-urlencoded")
}

func setupSingleExport(ctx context.Context, client client.Sender, export model.Export) (model.Export, error) {
	temp := []model.Export{export}
	temp, err := setupExports(ctx, client, temp)
	if err != nil {
		return model.Export{}, err
	}
	return temp[0], nil
}

// setupExports handles creating table and token for exports.
func setupExports(ctx context.Context, client client.Sender, exports []model.Export) ([]model.Export, error) {
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()

	// filter for unique buckets
	buckets := make(map[storageapi.BucketID]storageapi.TableID, 0)
	for _, export := range exports {
		buckets[export.Mapping.TableID.BucketID] = export.Mapping.TableID
	}
	// create all unique buckets in parallel
	// this step is separate because interleaving it with creating tables would cause race conditions
	for _, tableID := range buckets {
		tableID := tableID

		wg.Add(1)
		go func() {
			defer wg.Done()

			// create bucket if it does not exist
			bucketID := tableID.BucketID
			err := storageapi.GetBucketRequest(bucketID).SendOrErr(ctx, client)
			if err != nil {
				err := storageapi.CreateBucketRequest(&storageapi.Bucket{ID: bucketID}).SendOrErr(ctx, client)
				if err != nil {
					errs.Append(err)
				}
			}
		}()
	}

	wg.Wait()
	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	// Setup exports in parallel
	// This will create tables and generate tokens as necessary
	for i := range exports {
		export := &exports[i]

		wg.Add(1)
		go func() {
			defer wg.Done()
			// Create table if it doesn't exist, and check schema if it does
			err := setupMappingTable(ctx, client, export.Mapping)
			if err != nil {
				errs.Append(err)
				return
			}

			err = generateExportToken(ctx, client, export)
			if err != nil {
				errs.Append(err)
				return
			}
		}()
	}
	wg.Wait()
	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	return exports, nil
}

// setupMappingTable checks if the mapping output table exists.
//
// If it exists, it checks if the schema is correct.
// If it does not exist, it creates the table.
func setupMappingTable(ctx context.Context, client client.Sender, mapping model.Mapping) (err error) {
	columnNames := make([]string, 0, len(mapping.Columns))
	for _, column := range mapping.Columns {
		columnNames = append(columnNames, column.ColumnName())
	}

	tableID := mapping.TableID

	// check if table exists
	table, err := storageapi.GetTableRequest(tableID).Send(ctx, client)
	if err == nil {
		// table exists, check if columns match
		for i, name := range columnNames {
			if table.Columns[i] != name {
				return NewBadRequestError(errors.New("Export mapping does not match existing table schema."))
			}
		}
		// columns match, we can exit
		return nil
	}

	// create table
	_, err = storageapi.CreateTable(ctx, client, mapping.TableID, columnNames)
	if err != nil {
		return err
	}

	return nil
}

func generateExportToken(ctx context.Context, client client.Sender, export *model.Export) (err error) {
	token, err := storageapi.CreateTokenRequest(
		storageapi.WithDescription(
			// Max length of description is 255 characters,
			// this will be at most receiverId (48) + exportId (48) + extra chars (40) = 136 characters.
			fmt.Sprintf("[_internal] Buffer Export %s for Receiver %s", export.ReceiverKey.ReceiverID, export.ExportID),
		),
		storageapi.WithBucketPermission(
			export.Mapping.TableID.BucketID,
			storageapi.BucketPermissionWrite,
		),
	).Send(ctx, client)
	if err != nil {
		return err
	}

	export.Token = *token

	return nil
}
