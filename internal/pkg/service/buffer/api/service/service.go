package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	utilsUrl "github.com/keboola/keboola-as-code/internal/pkg/utils/url"
)

type service struct{}

func New() Service {
	return &service{}
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
	ctx, str := d.RequestCtx(), d.Store()

	receiver := model.ReceiverBase{
		ReceiverKey: key.ReceiverKey{
			ProjectID: d.ProjectID(),
		},
		Name: payload.Name,
	}

	// Generate receiver ID from Name if needed
	if payload.ID != nil && len(*payload.ID) != 0 {
		receiver.ReceiverID = strhelper.NormalizeName(string(*payload.ID))
	} else {
		receiver.ReceiverID = strhelper.NormalizeName(receiver.Name)
	}

	// Generate Secret
	receiver.Secret = idgenerator.ReceiverSecret()

	for _, exportData := range payload.Exports {
		export := model.ExportBase{
			Name:             exportData.Name,
			ImportConditions: model.DefaultConditions(),
		}
		export.ReceiverKey = receiver.ReceiverKey

		if exportData.Conditions != nil {
			export.ImportConditions.Count = exportData.Conditions.Count
			export.ImportConditions.Size, err = datasize.ParseString(exportData.Conditions.Size)
			if err != nil {
				return nil, NewBadRequestError(errors.Errorf(
					`value "%s" is not valid buffer size in bytes. Allowed units: B, kB, MB. For example: "5MB"`,
					exportData.Conditions.Size,
				))
			}
			export.ImportConditions.Time, err = time.ParseDuration(exportData.Conditions.Time)
			if err != nil {
				return nil, NewBadRequestError(errors.Errorf(
					`value "%s" is not valid time duration. Allowed units: s, m, h. For example: "30s"`,
					exportData.Conditions.Size,
				))
			}
		}

		// Generate export ID from Name if needed
		if exportData.ID != nil && len(*exportData.ID) != 0 {
			export.ExportID = string(*exportData.ID)
		} else {
			export.ExportID = strhelper.NormalizeName(export.Name)
		}

		// nolint:godox
		// TODO: create mappings

		tableID, err := model.ParseTableID(exportData.Mapping.TableID)
		if err != nil {
			return nil, err
		}
		columns := make([]column.Column, 0, len(exportData.Mapping.Columns))
		for _, columnData := range exportData.Mapping.Columns {
			c, err := column.TypeToColumn(columnData.Type)
			if err != nil {
				return nil, err
			}
			if template, ok := c.(column.Template); ok {
				if columnData.Template == nil {
					return nil, errors.Errorf("missing template column data")
				}
				template.Language = columnData.Template.Language
				template.UndefinedValueStrategy = columnData.Template.UndefinedValueStrategy
				template.DataType = columnData.Template.DataType
				template.Content = columnData.Template.Content
				c = template
			}
			columns = append(columns, c)
		}

		mapping := model.Mapping{
			TableID:     tableID,
			Incremental: exportData.Mapping.Incremental == nil || *exportData.Mapping.Incremental, // default true
			Columns:     columns,
		}
		mapping.ExportKey = export.ExportKey

		// Persist mapping
		err = str.CreateMapping(ctx, mapping)
		if err != nil {
			return nil, err
		}

		// Persist export
		if err := str.CreateExport(ctx, export); err != nil {
			return nil, err
		}
	}

	// Persist receiver
	if err := str.CreateReceiver(ctx, receiver); err != nil {
		return nil, err
	}
	return s.GetReceiver(d, &buffer.GetReceiverPayload{ReceiverID: ReceiverID(receiver.ReceiverID)})
}

func (s *service) UpdateReceiver(dependencies.ForProjectRequest, *UpdateReceiverPayload) (res *Receiver, err error) {
	return nil, NewNotImplementedError()
}

func (s *service) GetReceiver(d dependencies.ForProjectRequest, payload *GetReceiverPayload) (res *Receiver, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: string(payload.ReceiverID)}
	receiver, err := str.GetReceiver(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	exportList, err := str.ListExports(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	exports, err := mapExportsToPayload(ctx, str, exportList)
	if err != nil {
		return nil, err
	}

	url := formatUrl(d.BufferApiHost(), receiver.ProjectID, receiver.ReceiverID, receiver.Secret)
	resp := &buffer.Receiver{
		ID:      ReceiverID(receiver.ReceiverID),
		Name:    receiver.Name,
		URL:     url,
		Exports: exports,
	}

	return resp, nil
}

func (s *service) ListReceivers(d dependencies.ForProjectRequest, _ *buffer.ListReceiversPayload) (res *buffer.ReceiversList, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	projectID := d.ProjectID()

	receiverList, err := str.ListReceivers(ctx, projectID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list receivers in project \"%d\"", projectID)
	}

	bufferApiHost := d.BufferApiHost()

	receivers := make([]*buffer.Receiver, 0, len(receiverList))
	for _, receiverData := range receiverList {
		exportList, err := str.ListExports(ctx, receiverData.ReceiverKey)
		if err != nil {
			return nil, errors.Wrapf(err, `failed to list exports for receiver "%s"`, receiverData.ReceiverID)
		}

		exports, err := mapExportsToPayload(ctx, str, exportList)
		if err != nil {
			return nil, err
		}

		receivers = append(receivers, &buffer.Receiver{
			ID:      ReceiverID(receiverData.ReceiverID),
			Name:    receiverData.Name,
			URL:     formatUrl(bufferApiHost, receiverData.ProjectID, receiverData.ReceiverID, receiverData.Secret),
			Exports: exports,
		})
	}

	sort.SliceStable(receivers, func(i, j int) bool {
		return receivers[i].ID < receivers[j].ID
	})

	return &buffer.ReceiversList{Receivers: receivers}, nil
}

func (s *service) DeleteReceiver(d dependencies.ForProjectRequest, payload *buffer.DeleteReceiverPayload) (err error) {
	ctx, str := d.RequestCtx(), d.Store()

	// nolint:godox
	// TODO: delete export/mapping

	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: string(payload.ReceiverID)}
	if err := str.DeleteReceiver(ctx, receiverKey); err != nil {
		return err
	}

	return nil
}

func (s *service) RefreshReceiverTokens(dependencies.ForProjectRequest, *buffer.RefreshReceiverTokensPayload) (res *buffer.Receiver, err error) {
	return nil, NewNotImplementedError()
}

func (s *service) CreateExport(dependencies.ForProjectRequest, *buffer.CreateExportPayload) (res *buffer.Export, err error) {
	return nil, NewNotImplementedError()
}

func (s *service) UpdateExport(dependencies.ForProjectRequest, *buffer.UpdateExportPayload) (res *buffer.Export, err error) {
	return nil, NewNotImplementedError()
}

func (s *service) DeleteExport(dependencies.ForProjectRequest, *buffer.DeleteExportPayload) (err error) {
	return NewNotImplementedError()
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

	exports, err := str.ListExports(d.RequestCtx(), receiver.ReceiverKey)
	if err != nil {
		return err
	}

	importCtx := column.NewImportCtx(data, header, ip)
	receivedAt := time.Now()

	errs := errors.NewMultiError()
	for _, e := range exports {
		mapping, err := str.GetLatestMapping(ctx, e.ExportKey)
		if err != nil {
			return err
		}
		csv := make([]string, 0)
		for _, c := range mapping.Columns {
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

func formatUrl(bufferApiHost string, projectID int, receiverID string, secret string) string {
	return fmt.Sprintf("https://%s/v1/import/%d/%s/%s", bufferApiHost, projectID, receiverID, secret)
}

func mapExportsToPayload(ctx context.Context, str *store.Store, exportList []model.ExportBase) ([]*Export, error) {
	exports := make([]*Export, 0, len(exportList))
	for _, export := range exportList {
		mapping, err := str.GetLatestMapping(ctx, export.ExportKey)
		if err != nil {
			return nil, err
		}

		columns := make([]*Column, 0, len(mapping.Columns))
		for _, c := range mapping.Columns {
			var template *Template
			if v, ok := c.(column.Template); ok {
				template = &Template{
					Language:               v.Language,
					UndefinedValueStrategy: v.UndefinedValueStrategy,
					Content:                v.Content,
					DataType:               v.DataType,
				}
			}
			typ, _ := column.ColumnToType(c)
			columns = append(columns, &Column{
				Type:     typ,
				Template: template,
			})
		}

		exports = append(exports, &Export{
			ID:         ExportID(export.ExportID),
			ReceiverID: ReceiverID(export.ReceiverID),
			Name:       export.Name,
			Mapping: &Mapping{
				TableID:     mapping.TableID.String(),
				Incremental: &mapping.Incremental,
				Columns:     columns,
			},
			Conditions: &Conditions{
				Count: export.ImportConditions.Count,
				Size:  export.ImportConditions.Size.String(),
				Time:  export.ImportConditions.Time.String(),
			},
		})
	}
	return exports, nil
}
