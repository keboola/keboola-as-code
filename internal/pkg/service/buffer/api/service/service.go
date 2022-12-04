package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	utilsUrl "github.com/keboola/keboola-as-code/internal/pkg/utils/url"
)

type service struct {
	mapper mapper.Mapper
}

type serviceDeps interface {
	BufferAPIHost() string
}

func New(d serviceDeps) Service {
	return &service{mapper: mapper.NewMapper(d.BufferAPIHost())}
}

func (s *service) APIRootIndex(dependencies.ForPublicRequest) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(dependencies.ForPublicRequest) (res *buffer.ServiceDetail, err error) {
	res = &ServiceDetail{
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
	receiver, err := mpr.ReceiverModelFromPayload(d.ProjectID(), secret, *payload)
	if err != nil {
		return nil, err
	}

	// Create export tables as necessary and generate tokens
	wg := &sync.WaitGroup{}
	errors := errors.NewMultiError()
	for i := range receiver.Exports {
		export := &receiver.Exports[i]

		wg.Add(1)
		go func() {
			defer wg.Done()

			err := setupExport(ctx, d.StorageAPIClient(), export)
			if err != nil {
				errors.Append(err)
			}
		}()
	}
	wg.Wait()
	if err = errors.ErrorOrNil(); err != nil {
		return nil, err
	}

	// Persist receiver
	if err := str.CreateReceiver(ctx, receiver); err != nil {
		return nil, err
	}

	return s.GetReceiver(d, &buffer.GetReceiverPayload{ReceiverID: ReceiverID(receiver.ReceiverID)})
}

func (s *service) UpdateReceiver(d dependencies.ForProjectRequest, payload *UpdateReceiverPayload) (res *Receiver, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	// Get export
	receiverKey := key.ReceiverKey{
		ProjectID:  d.ProjectID(),
		ReceiverID: string(payload.ReceiverID),
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

func (s *service) GetReceiver(d dependencies.ForProjectRequest, payload *GetReceiverPayload) (res *Receiver, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: string(payload.ReceiverID)}

	receiver, err := str.GetReceiver(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	resp := mpr.ReceiverPayloadFromModel(receiver)

	return &resp, nil
}

func (s *service) ListReceivers(d dependencies.ForProjectRequest, _ *buffer.ListReceiversPayload) (res *buffer.ReceiversList, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	projectID := d.ProjectID()

	model, err := str.ListReceivers(ctx, projectID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list receivers in project \"%d\"", projectID)
	}

	receivers := make([]*Receiver, 0, len(model))
	for _, data := range model {
		receiver := mpr.ReceiverPayloadFromModel(data)
		receivers = append(receivers, &receiver)
	}

	return &buffer.ReceiversList{Receivers: receivers}, nil
}

func (s *service) DeleteReceiver(d dependencies.ForProjectRequest, payload *buffer.DeleteReceiverPayload) (err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: string(payload.ReceiverID)}
	if err := str.DeleteReceiver(ctx, receiverKey); err != nil {
		return err
	}

	return nil
}

func (s *service) RefreshReceiverTokens(d dependencies.ForProjectRequest, payload *buffer.RefreshReceiverTokensPayload) (res *buffer.Receiver, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: string(payload.ReceiverID)}
	tokens, err := str.ListTokens(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	refreshedTokens := make([]model.TokenForExport, 0, len(tokens))
	for _, token := range tokens {
		refreshed, err := storageapi.RefreshTokenRequest(token.Token.ID).Send(ctx, d.StorageAPIClient())
		if err != nil {
			return nil, err
		}
		refreshedTokens = append(refreshedTokens, model.TokenForExport{
			ExportKey: token.ExportKey,
			Token:     *refreshed,
		})
	}

	err = str.UpdateTokens(ctx, refreshedTokens)
	if err != nil {
		return nil, err
	}

	return s.GetReceiver(d, &GetReceiverPayload{ReceiverID: payload.ReceiverID})
}

func (s *service) CreateExport(d dependencies.ForProjectRequest, payload *buffer.CreateExportPayload) (res *buffer.Export, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	// Map payload to export
	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: string(payload.ReceiverID)}
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
	err = setupExport(ctx, d.StorageAPIClient(), &export)
	if err != nil {
		return nil, err
	}

	// Persist export
	if err := str.CreateExport(ctx, export); err != nil {
		return nil, err
	}

	return s.GetExport(d, &buffer.GetExportPayload{
		ReceiverID: ReceiverID(export.ReceiverID),
		ExportID:   ExportID(export.ExportID),
	})
}

func (s *service) UpdateExport(d dependencies.ForProjectRequest, payload *buffer.UpdateExportPayload) (res *buffer.Export, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	// Get export
	exportKey := key.ExportKey{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  d.ProjectID(),
			ReceiverID: string(payload.ReceiverID),
		},
		ExportID: string(payload.ExportID),
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

	// Persist
	err = str.UpdateExport(ctx, export)
	if err != nil {
		return nil, err
	}

	return s.GetExport(d, &buffer.GetExportPayload{
		ReceiverID: ReceiverID(export.ReceiverID),
		ExportID:   ExportID(export.ExportID),
	})
}

func (s *service) GetExport(d dependencies.ForProjectRequest, payload *buffer.GetExportPayload) (r *buffer.Export, err error) {
	ctx, str, mpr := d.RequestCtx(), d.Store(), s.mapper

	exportKey := key.ExportKey{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  d.ProjectID(),
			ReceiverID: string(payload.ReceiverID),
		},
		ExportID: string(payload.ExportID),
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
		ProjectID:  d.ProjectID(),
		ReceiverID: string(payload.ReceiverID),
	}

	model, err := str.ListExports(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	exports := make([]*Export, 0, len(model))
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
			ProjectID:  d.ProjectID(),
			ReceiverID: string(payload.ReceiverID),
		},
		ExportID: string(payload.ExportID),
	}
	if err := str.DeleteExport(ctx, exportKey); err != nil {
		return err
	}

	return nil
}

func (*service) Import(d dependencies.ForPublicRequest, payload *buffer.ImportPayload, reader io.ReadCloser) (err error) {
	ctx, str, header, ip := d.RequestCtx(), d.Store(), d.RequestHeader(), d.RequestClientIP()

	receiverKey := key.ReceiverKey{ProjectID: payload.ProjectID, ReceiverID: string(payload.ReceiverID)}
	receiver, err := str.GetReceiver(ctx, receiverKey)
	if err != nil {
		return err
	}
	if receiver.Secret != payload.Secret {
		return &GenericError{
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
		csv := make([]string, 0)
		for _, c := range e.Mapping.Columns {
			csvValue, err := c.CsvValue(importCtx)
			if err != nil {
				return err
			}
			csv = append(csv, csvValue)
		}

		// nolint:godox
		// TODO get fileID and sliceID

		record := key.NewRecordKey(e.ProjectID, e.ReceiverID, e.ExportID, "file", "slice", receivedAt)
		err = str.CreateRecord(ctx, record, csv)
		if err != nil {
			errs.AppendWithPrefixf(err, `failed to create record for export "%s"`, e.ExportID)
		}
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

// setupExport handles creating table and token for an export
func setupExport(ctx context.Context, client client.Sender, export *model.Export) (err error) {
	// Create table if it doesn't exist, and check schema if it does
	err = setupMappingTable(ctx, client, export.Mapping)
	if err != nil {
		return err
	}

	// Generate token
	token, err := storageapi.CreateTokenRequest(
		storageapi.WithBucketPermission(
			storageapi.BucketID(export.Mapping.TableID.BucketID()),
			storageapi.BucketPermissionWrite,
		),
	).Send(ctx, client)
	if err != nil {
		return err
	}
	export.Token = *token

	return nil
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

	bucketID := storageapi.BucketID(mapping.TableID.BucketID())
	tableID := storageapi.TableID(mapping.TableID.String())

	// check if table exists
	table, err := storageapi.GetTableRequest(tableID).Send(ctx, client)
	if err == nil && table != nil {
		// table exists, check if columns match
		for i, name := range columnNames {
			if table.Columns[i] != name {
				return NewBadRequestError(errors.Errorf("export mapping does not match existing table schema"))
			}
		}
		// columns match, we can exit
		return nil
	}

	// table does not exist
	// create bucket if it does not exist
	err = storageapi.GetBucketRequest(bucketID).SendOrErr(ctx, client)
	if err != nil {
		bucket := &storageapi.Bucket{
			ID:    bucketID,
			Stage: mapping.TableID.Stage,
		}
		err = storageapi.CreateBucketRequest(bucket).SendOrErr(ctx, client)
	}

	// create table
	err = storageapi.CreateTable(ctx, client, string(bucketID), mapping.TableID.Table, columnNames)
	if err != nil {
		return err
	}

	return nil
}
