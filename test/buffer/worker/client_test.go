package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/request"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"

	bufferDesign "github.com/keboola/keboola-as-code/api/buffer"
	apiModel "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	apiServer "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/http/buffer/server"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// apiClient is part of testSuite, it provides API requests to Buffer and Storage APIs.
type apiClient struct {
	t   *testing.T
	ctx context.Context
	ts  *testSuite
}

type Error struct {
	Message string `json:"message"`
}

func (e Error) Error() string {
	return e.Message
}

func newAPIClient(ts *testSuite) *apiClient {
	return &apiClient{t: ts.t, ctx: ts.ctx, ts: ts}
}

func (c *apiClient) newRequest() request.HTTPRequest {
	return request.NewHTTPRequest(c.ts.RandomAPINode().APIClient).WithError(&Error{})
}

func (c *apiClient) newRequestWithToken() request.HTTPRequest {
	return c.newRequest().AndHeader("X-StorageAPI-Token", c.ts.project.StorageAPIToken().Token)
}

// TablePreview loads preview of the table, sorted by the "id" column.
func (c *apiClient) TablePreview(tableID, sortBy string) *keboola.TablePreview {
	c.t.Helper()
	c.t.Logf(`loading preview of the table "%s" ...`, tableID)

	preview, err := c.ts.project.KeboolaProjectAPI().
		PreviewTableRequest(
			keboola.MustParseTableID(tableID),
			keboola.WithLimitRows(20),
			keboola.WithOrderBy(sortBy, keboola.OrderAsc),
		).
		Send(c.ctx)

	if assert.NoError(c.t, err) {
		c.t.Logf(`loaded preview of the table "%s", found %d rows`, tableID, len(preview.Rows))
	}

	// Replace random dates
	for i, row := range preview.Rows {
		for j := range row {
			col := &preview.Rows[i][j]
			if regexpcache.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$`).MatchString(*col) {
				*col = "<date>"
			}
		}
	}

	return preview
}

func (c *apiClient) SendPayload(id int) {
	c.t.Helper()
	assert.NoError(c.t, c.SendPayloadBody(fmt.Sprintf(`{"key": "payload%03d"}`, id)))
}

func (c *apiClient) SendPayloadBody(body string) error {
	c.t.Helper()
	size := datasize.ByteSize(len(body)).HumanReadable()
	c.t.Logf(`sending payload %s (%s) ...`, strhelper.Truncate(body, 40, "…"), size)

	time.Sleep(time.Millisecond) // prevent order issues
	err := c.newRequest().
		WithPost("v1/import/{projectId}/{receiverId}/{secret}").
		AndPathParam("projectId", cast.ToString(c.ts.project.ID())).
		AndPathParam("receiverId", c.ts.receiver.ID.String()).
		AndPathParam("secret", c.ts.secret).
		AndHeader("Content-Type", "application/json").
		WithBody(body).
		SendOrErr(c.ctx)
	if err != nil {
		c.t.Logf(`send payload failed: %s`, err)
	}

	return err
}

func (c *apiClient) CreateReceiver(name string) *apiModel.Receiver {
	c.t.Helper()
	task := c.CreateReceiverAsync(name)
	task = c.WaitForBufferTask(task.ID)
	var receiverID apiModel.ReceiverID
	if task.Outputs.ReceiverID != nil {
		receiverID = *task.Outputs.ReceiverID
	}

	if assert.Nil(c.t, task.Error) {
		c.t.Logf(`created receiver "%s"`, receiverID)
	}

	return c.GetReceiver(receiverID)
}

func (c *apiClient) CreateReceiverAsync(name string) *apiModel.Task {
	c.t.Helper()
	receiverID := apiModel.ReceiverID(strhelper.NormalizeName(name))
	c.t.Logf(`creating receiver "%s" ...`, receiverID)

	task := &apiModel.Task{}
	err := c.newRequestWithToken().
		WithPost("v1/receivers").
		AndHeader("Content-Type", "application/json").
		WithBody(&apiServer.CreateReceiverRequestBody{Name: &name}).
		WithResult(task).
		SendOrErr(c.ctx)

	if assert.NoError(c.t, err) {
		c.t.Logf(`created task: create receiver "%s"`, receiverID)
	}

	return task
}

func (c *apiClient) GetReceiver(receiverID apiModel.ReceiverID) *apiModel.Receiver {
	c.t.Helper()
	c.t.Logf(`loading receiver "%s" ...`, receiverID)

	receiver := &apiModel.Receiver{}
	err := c.newRequestWithToken().
		WithGet("v1/receivers/{receiverId}").
		AndPathParam("receiverId", receiverID.String()).
		WithResult(receiver).
		SendOrErr(c.ctx)

	if assert.NoError(c.t, err) {
		c.t.Logf(`loaded receiver "%s"`, receiverID)
	}

	require.NotEmpty(c.t, receiver)

	return receiver
}

func (c *apiClient) DeleteReceiver(receiverID apiModel.ReceiverID) {
	c.t.Helper()
	c.t.Logf(`deleting receiver "%s" ...`, receiverID)

	err := c.newRequestWithToken().
		WithDelete("v1/receivers/{receiverId}").
		AndPathParam("receiverId", receiverID.String()).
		SendOrErr(c.ctx)

	if assert.NoError(c.t, err) {
		c.t.Logf(`deleted receiver "%s"`, receiverID)
	}
}

func (c *apiClient) CreateExport(receiver *apiModel.Receiver, name string, columns []*apiServer.ColumnRequestBody) *apiModel.Export {
	c.t.Helper()
	task := c.CreateExportAsync(receiver, name, columns)
	task = c.WaitForBufferTask(task.ID)
	var exportID apiModel.ExportID
	if task.Outputs.ReceiverID != nil {
		exportID = *task.Outputs.ExportID
	}

	if assert.Nil(c.t, task.Error) {
		c.t.Logf(`created export "%s/%s"`, receiver.ID, exportID)
	}

	return c.GetExport(receiver.ID, exportID)
}

func (c *apiClient) CreateExportAsync(receiver *apiModel.Receiver, name string, columns []*apiServer.ColumnRequestBody) *apiModel.Task {
	c.t.Helper()
	exportID := apiModel.ExportID(strhelper.NormalizeName(name))
	c.t.Logf(`creating export "%s/%s" ...`, receiver.ID, exportID)

	// Start a "create export" task
	task := &apiModel.Task{}
	tableID := "in.c-bucket." + strhelper.NormalizeName(name)
	conditionCount := importConditionsCount
	conditionSize := importConditionsSize.String()
	conditionTime := importConditionsTime.String()
	err := c.newRequestWithToken().
		WithPost("v1/receivers/{receiverId}/exports").
		AndPathParam("receiverId", receiver.ID.String()).
		AndHeader("Content-Type", "application/json").
		WithBody(&apiServer.CreateExportRequestBody{
			Name:       &name,
			Mapping:    &apiServer.MappingRequestBody{TableID: &tableID, Columns: columns},
			Conditions: &apiServer.ConditionsRequestBody{Count: &conditionCount, Size: &conditionSize, Time: &conditionTime},
		}).
		WithResult(task).
		SendOrErr(c.ctx)

	if assert.NoError(c.t, err) {
		c.t.Logf(`created task: create export "%s/%s"`, receiver.ID, exportID)
	}

	return task
}

func (c *apiClient) UpdateExport(receiverID apiModel.ReceiverID, exportID apiModel.ExportID, mapping *apiServer.MappingRequestBody) *apiModel.Export {
	c.t.Helper()
	task := c.UpdateExportAsync(receiverID, exportID, mapping)
	task = c.WaitForBufferTask(task.ID)

	if assert.Nil(c.t, task.Error) {
		c.t.Logf(`updated export "%s/%s"`, receiverID, exportID)
	}

	return c.GetExport(receiverID, exportID)
}

func (c *apiClient) UpdateExportAsync(receiverID apiModel.ReceiverID, exportID apiModel.ExportID, mapping *apiServer.MappingRequestBody) *apiModel.Task {
	c.t.Helper()
	c.t.Logf(`updating export "%s/%s" ...`, receiverID, exportID)

	task := &apiModel.Task{}
	err := c.newRequestWithToken().
		WithPatch("v1/receivers/{receiverID}/exports/{exportID}").
		AndPathParam("receiverID", receiverID.String()).
		AndPathParam("exportID", exportID.String()).
		AndHeader("Content-Type", "application/json").
		WithBody(&apiServer.UpdateExportRequestBody{Mapping: mapping}).
		WithResult(task).
		SendOrErr(c.ctx)

	if assert.NoError(c.t, err) {
		c.t.Logf(`created task: update export "%s/%s"`, receiverID, exportID)
	}

	return task
}

func (c *apiClient) GetExport(receiverID apiModel.ReceiverID, exportID apiModel.ExportID) *apiModel.Export {
	c.t.Helper()
	c.t.Logf(`loading export "%s/%s" ...`, receiverID, exportID)

	export := &apiModel.Export{}
	err := c.newRequestWithToken().
		WithGet("v1/receivers/{receiverId}/exports/{exportId}").
		AndPathParam("receiverId", receiverID.String()).
		AndPathParam("exportId", exportID.String()).
		WithResult(export).
		SendOrErr(c.ctx)

	if assert.NoError(c.t, err) {
		c.t.Logf(`loaded export "%s/%s"`, receiverID, exportID)
	}

	require.NotEmpty(c.t, export)

	return export
}

func (c *apiClient) DeleteExport(receiverID apiModel.ReceiverID, exportID apiModel.ExportID) {
	c.t.Helper()
	c.t.Logf(`deleting export "%s/%s" ...`, receiverID, exportID)

	err := c.newRequestWithToken().
		WithDelete("v1/receivers/{receiverId}/exports/{exportId}").
		AndHeader("Content-Type", "application/json").
		AndPathParam("receiverId", receiverID.String()).
		AndPathParam("exportId", exportID.String()).
		SendOrErr(c.ctx)

	if assert.NoError(c.t, err) {
		c.t.Logf(`deleted export "%s/%s"`, receiverID, exportID)
	}
}

func (c *apiClient) WaitForBufferTask(taskID apiModel.TaskID) *apiModel.Task {
	c.t.Helper()
	c.t.Logf(`waiting for task "%s" ...`, taskID)

	task := &apiModel.Task{}
	assert.Eventually(c.t, func() bool {
		assert.NoError(
			c.t,
			c.newRequestWithToken().
				WithGet("v1/tasks/"+taskID.String()).
				WithResult(task).
				SendOrErr(c.ctx),
		)
		return task.Status != bufferDesign.TaskStatusProcessing
	}, 30*time.Second, 100*time.Millisecond)
	return task
}
