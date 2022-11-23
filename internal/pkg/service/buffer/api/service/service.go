package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/configstore"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/httperror"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	utilsUrl "github.com/keboola/keboola-as-code/internal/pkg/utils/url"
)

type service struct{}

func New() Service {
	return &service{}
}

func (*service) APIRootIndex(dependencies.ForPublicRequest) (err error) {
	// Redirect / -> /v1
	return nil
}

func (*service) APIVersionIndex(dependencies.ForPublicRequest) (res *buffer.ServiceDetail, err error) {
	res = &ServiceDetail{
		API:           "buffer",
		Documentation: "https://buffer.keboola.com/v1/documentation",
	}
	return res, nil
}

func (*service) HealthCheck(dependencies.ForPublicRequest) (res string, err error) {
	return "OK", nil
}

// nolint:godox
// TODO: collect errors instead of bailing on the first one

func (*service) CreateReceiver(d dependencies.ForProjectRequest, payload *buffer.CreateReceiverPayload) (res *buffer.ReceiverResponse, err error) {
	ctx, store := d.RequestCtx(), d.ConfigStore()

	receiver := model.Receiver{
		ProjectID: d.ProjectID(),
		Name:      payload.Name,
	}

	// Generate receiver ID from Name if needed
	if payload.ReceiverID != nil && len(*payload.ReceiverID) != 0 {
		receiver.ID = *payload.ReceiverID
	} else {
		receiver.ID = strhelper.NormalizeName(receiver.Name)
	}

	// Generate Secret
	receiver.Secret = idgenerator.ReceiverSecret()

	for _, exportData := range payload.Exports {
		export := model.Export{
			Name: exportData.Name,
			ImportConditions: model.ImportConditions{
				Count: 1,
				Size:  100,
				Time:  30 * time.Second,
			},
		}

		if exportData.Conditions != nil {
			export.ImportConditions.Count = exportData.Conditions.Count
			export.ImportConditions.Size = datasize.ByteSize(exportData.Conditions.Size)
			export.ImportConditions.Time = time.Duration(exportData.Conditions.Time) * time.Second
		}

		// Generate export ID from Name if needed
		if exportData.ExportID != nil && len(*exportData.ExportID) != 0 {
			export.ID = *exportData.ExportID
		} else {
			export.ID = strhelper.NormalizeName(export.Name)
		}

		// nolint:godox
		// TODO: create mappings

		tableId, err := model.ParseTableID(exportData.Mapping.TableID)
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
			RevisionID:  1,
			TableID:     tableId,
			Incremental: exportData.Mapping.Incremental,
			Columns:     columns,
		}

		// Persist mapping
		err = store.CreateMapping(ctx, receiver.ProjectID, receiver.ID, export.ID, mapping)
		if err != nil {
			return nil, err
		}

		// Persist export
		err = store.CreateExport(ctx, receiver.ProjectID, receiver.ID, export)
		if err != nil {
			if errors.As(err, &configstore.LimitReachedError{}) {
				return nil, &GenericError{
					StatusCode: http.StatusUnprocessableEntity,
					Name:       "buffer.resourceLimitReached",
					Message:    fmt.Sprintf("Maximum number of exports per receiver is %d.", configstore.MaxExportsPerReceiver),
				}
			}
			if errors.As(err, &configstore.AlreadyExistsError{}) {
				return nil, &GenericError{
					StatusCode: http.StatusConflict,
					Name:       "buffer.alreadyExists",
					Message:    fmt.Sprintf(`Export "%s" already exists.`, export.ID),
				}
			}
			// nolint:godox
			// TODO: maybe handle validation error
			return nil, err
		}
	}

	// Persist receiver
	err = store.CreateReceiver(ctx, receiver)
	if err != nil {
		if errors.As(err, &configstore.LimitReachedError{}) {
			return nil, &GenericError{
				StatusCode: http.StatusUnprocessableEntity,
				Name:       "buffer.resourceLimitReached",
				Message:    fmt.Sprintf("Maximum number of receivers per project is %d.", configstore.MaxReceiversPerProject),
			}
		}
		if errors.As(err, &configstore.AlreadyExistsError{}) {
			return nil, &GenericError{
				StatusCode: http.StatusConflict,
				Name:       "buffer.alreadyExists",
				Message:    fmt.Sprintf(`Receiver "%s" already exists.`, receiver.ID),
			}
		}
		// nolint:godox
		// TODO: maybe handle validation error
		return nil, errors.Wrapf(err, "failed to create receiver \"%s\"", receiver.ID)
	}

	url := formatUrl(d.BufferApiHost(), receiver.ProjectID, receiver.ID, receiver.Secret)
	resp := &buffer.ReceiverResponse{
		ReceiverID: receiver.ID,
		Name:       receiver.Name,
		URL:        url,
		Exports:    payload.Exports,
	}

	return resp, nil
}

func (*service) GetReceiver(d dependencies.ForProjectRequest, payload *buffer.GetReceiverPayload) (res *buffer.ReceiverResponse, err error) {
	ctx, store := d.RequestCtx(), d.ConfigStore()

	projectID, receiverID := d.ProjectID(), payload.ReceiverID

	receiver, err := store.GetReceiver(ctx, projectID, receiverID)
	if err != nil {
		if errors.As(err, &configstore.NotFoundError{}) {
			return nil, &GenericError{
				StatusCode: http.StatusNotFound,
				Name:       "buffer.receiverNotFound",
				Message:    fmt.Sprintf("Receiver \"%s\" not found", receiverID),
			}
		}
		return nil, errors.Wrapf(err, "failed to get receiver \"%s\" in project \"%d\"", receiverID, projectID)
	}

	exportList, err := store.ListExports(ctx, projectID, receiverID)
	if err != nil {
		return nil, err
	}

	exports := make([]*Export, 0, len(exportList))
	for _, export := range exportList {
		mapping, err := store.GetCurrentMapping(ctx, projectID, receiverID, export.ID)
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
			ExportID: &export.ID,
			Name:     export.Name,
			Mapping: &Mapping{
				TableID:     mapping.TableID.String(),
				Incremental: mapping.Incremental,
				Columns:     columns,
			},
			Conditions: &Conditions{
				Count: export.ImportConditions.Count,
				Size:  int(export.ImportConditions.Size),
				Time:  int(export.ImportConditions.Time / time.Second),
			},
		})
	}

	sort.SliceStable(exports, func(i, j int) bool {
		return *exports[i].ExportID < *exports[j].ExportID
	})

	url := formatUrl(d.BufferApiHost(), receiver.ProjectID, receiver.ID, receiver.Secret)
	resp := &buffer.ReceiverResponse{
		ReceiverID: receiver.ID,
		Name:       receiver.Name,
		URL:        url,
		Exports:    exports,
	}

	return resp, nil
}

func (*service) ListReceivers(d dependencies.ForProjectRequest, _ *buffer.ListReceiversPayload) (res *buffer.ListReceiversResult, err error) {
	ctx, store := d.RequestCtx(), d.ConfigStore()

	projectID := d.ProjectID()

	receiverList, err := store.ListReceivers(ctx, projectID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list receivers in project \"%d\"", projectID)
	}

	bufferApiHost := d.BufferApiHost()

	receivers := make([]*buffer.ReceiverResponse, 0, len(receiverList))
	for _, receiverData := range receiverList {
		exportList, err := store.ListExports(ctx, projectID, receiverData.ID)
		if err != nil {
			return nil, errors.Wrapf(err, `failed to list exports for receiver "%s"`, receiverData.ID)
		}

		exports := make([]*Export, 0, len(exportList))
		for _, export := range exportList {
			mapping, err := store.GetCurrentMapping(ctx, projectID, receiverData.ID, export.ID)
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
				ExportID: &export.ID,
				Name:     export.Name,
				Mapping: &Mapping{
					TableID:     mapping.TableID.String(),
					Incremental: mapping.Incremental,
					Columns:     columns,
				},
				Conditions: &Conditions{
					Count: export.ImportConditions.Count,
					Size:  int(export.ImportConditions.Size),
					Time:  int(export.ImportConditions.Time / time.Second),
				},
			})
		}

		sort.SliceStable(exports, func(i, j int) bool {
			return *exports[i].ExportID < *exports[j].ExportID
		})

		url := formatUrl(bufferApiHost, receiverData.ProjectID, receiverData.ID, receiverData.Secret)
		receivers = append(receivers, &buffer.ReceiverResponse{
			ReceiverID: receiverData.ID,
			Name:       receiverData.Name,
			URL:        url,
			Exports:    exports,
		})
	}

	sort.SliceStable(receivers, func(i, j int) bool {
		return receivers[i].ReceiverID < receivers[j].ReceiverID
	})

	return &buffer.ListReceiversResult{Receivers: receivers}, nil
}

func (*service) DeleteReceiver(d dependencies.ForProjectRequest, payload *buffer.DeleteReceiverPayload) (err error) {
	ctx, store := d.RequestCtx(), d.ConfigStore()

	projectID, receiverID := d.ProjectID(), payload.ReceiverID

	// nolint:godox
	// TODO: delete export/mapping

	err = store.DeleteReceiver(ctx, projectID, receiverID)
	if err != nil {
		if errors.As(err, &configstore.NotFoundError{}) {
			return &GenericError{
				StatusCode: 404,
				Name:       "buffer.receiverNotFound",
				Message:    fmt.Sprintf("Receiver \"%s\" not found", receiverID),
			}
		}
		return err
	}

	return nil
}

func (*service) RefreshReceiverTokens(dependencies.ForProjectRequest, *buffer.RefreshReceiverTokensPayload) (res *buffer.ReceiverResponse, err error) {
	return nil, &NotImplementedError{}
}

func (*service) CreateExport(dependencies.ForProjectRequest, *buffer.CreateExportPayload) (res *buffer.ReceiverResponse, err error) {
	return nil, &NotImplementedError{}
}

func (*service) UpdateExport(dependencies.ForProjectRequest, *buffer.UpdateExportPayload) (res *buffer.ReceiverResponse, err error) {
	return nil, &NotImplementedError{}
}

func (*service) DeleteExport(dependencies.ForProjectRequest, *buffer.DeleteExportPayload) (res *buffer.ReceiverResponse, err error) {
	return nil, &NotImplementedError{}
}

func (*service) Import(dependencies.ForPublicRequest, *buffer.ImportPayload, io.ReadCloser) (err error) {
	return &NotImplementedError{}
}

func parseRequestBody(contentType string, reader io.ReadCloser) (res *orderedmap.OrderedMap, err error) {
	// Limit read csv to 1 MB plus 1 B. If the reader fills the limit then the request is bigger than allowed.
	limitedReader := io.LimitReader(reader, configstore.MaxImportRequestSizeInBytes+1)
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
	if buf.Len() > configstore.MaxImportRequestSizeInBytes {
		return nil, &PayloadTooLargeError{Message: "Payload too large."}
	}

	var data *orderedmap.OrderedMap
	if isContentTypeForm(contentType) {
		data, err = utilsUrl.ParseQuery(buf.String())
		if err != nil {
			return nil, &BadRequestError{Message: "Could not parse form request body."}
		}
	} else {
		err = json.Unmarshal([]byte(buf.String()), &data)
		if err != nil {
			return nil, &BadRequestError{Message: "Could not parse json request body."}
		}
	}
	return data, nil
}

func isContentTypeForm(t string) bool {
	return strings.HasPrefix(t, "application/x-www-form-urlencoded")
}

func formatUrl(bufferApiHost string, projectID int, receiverID string, secret string) string {
	return fmt.Sprintf("https://%s/v1/import/%d/%s/#/%s", bufferApiHost, projectID, receiverID, secret)
}
