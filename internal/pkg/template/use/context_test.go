package use_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	. "github.com/keboola/keboola-as-code/internal/pkg/template/use"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
)

func TestContext(t *testing.T) {
	t.Parallel()

	// Mocked ticket provider
	storageApi, httpTransport := testapi.NewMockedStorageApi(log.NewDebugLogger())
	tickets := storageApi.NewTicketProvider()

	// Mocked tickets
	var ticketResponses []*http.Response
	for i := 1; i <= 2; i++ {
		response, err := httpmock.NewJsonResponse(200, map[string]interface{}{"id": cast.ToString(1000 + i)})
		assert.NoError(t, err)
		ticketResponses = append(ticketResponses, response)
	}
	httpTransport.RegisterResponder("POST", `=~/storage/tickets`, httpmock.ResponderFromMultipleResponses(ticketResponses))

	// Inputs
	targetBranch := model.BranchKey{Id: 123}
	inputsValues := template.InputsValues{
		{
			Id:    "input-1",
			Value: "my-value-1",
		},
		{
			Id:    "input-2",
			Value: 789,
		},
		{
			Id:    "input-3",
			Value: 3.50,
		},
		{
			Id:    "input-4",
			Value: false,
		},
		{
			Id:      "input-5",
			Value:   "",
			Skipped: true,
		},
	}

	// Create context
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceId := "my-instance"
	fs := testfs.NewMemoryFs()

	// Enable inputUsageNotifier
	objectKey := model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", Id: "456"}
	fileDef := filesystem.NewFileDef("foo.bar")
	fileDef.AddMetadata(filesystem.ObjectKeyMetadata, objectKey)
	fileDef.AddTag(model.FileKindObjectConfig)
	ctx := context.WithValue(context.Background(), jsonnetfiles.FileDefCtxKey, fileDef)

	// Create template use context
	useCtx := NewContext(ctx, templateRef, fs, instanceId, targetBranch, inputsValues, tickets)

	// Check JsonNet functions
	code := `
{
	Input1: Input("input-1"),
    Input2: Input("input-2"),
    Input3: Input("input-3"),
    Input4: Input("input-4"),
    Input5: Input("input-5"),
    Objects: {
      Config1: ConfigId("my-config"),
      Config2: ConfigId("my-config"),
      Row1: ConfigRowId("my-row"),
      Row2: ConfigRowId("my-row"),
    },
}
`
	expectedJson := `
{
  "Input1": "my-value-1",
  "Input2": 789,
  "Input3": 3.5,
  "Input4": false,
  "Input5": "",
  "Objects": {
    "Config1": "<<~~placeholder:1~~>>",
    "Config2": "<<~~placeholder:1~~>>",
    "Row1": "<<~~placeholder:2~~>>",
    "Row2": "<<~~placeholder:2~~>>"
  }
}
`
	jsonOutput, err := jsonnet.Evaluate(code, useCtx.JsonNetContext())
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expectedJson, "\n"), jsonOutput)

	// Check tickets replacement
	data := orderedmap.New()
	json.MustDecodeString(jsonOutput, data)
	replacements, err := useCtx.Replacements()
	assert.NoError(t, err)
	modifiedData, err := replacements.Replace(data)
	assert.NoError(t, err)
	modifiedJson := json.MustEncodeString(modifiedData, true)

	expectedJson = `
{
  "Input1": "my-value-1",
  "Input2": 789,
  "Input3": 3.5,
  "Input4": false,
  "Input5": "",
  "Objects": {
    "Config1": "1001",
    "Config2": "1001",
    "Row1": "1002",
    "Row2": "1002"
  }
}
`
	assert.Equal(t, strings.TrimLeft(expectedJson, "\n"), modifiedJson)

	// Check collected inputs usage
	assert.Equal(t, &metadata.InputsUsage{
		Values: metadata.InputsUsageMap{
			objectKey: []metadata.InputUsage{
				{
					Name:    "input-1",
					JsonKey: orderedmap.KeyFromStr("Input1"),
				},
				{
					Name:    "input-2",
					JsonKey: orderedmap.KeyFromStr("Input2"),
				},
				{
					Name:    "input-3",
					JsonKey: orderedmap.KeyFromStr("Input3"),
				},
				{
					Name:    "input-4",
					JsonKey: orderedmap.KeyFromStr("Input4"),
				},
				// "input-5" IsSkipped, so it is not present here, it was not filled in by the user.
			},
		},
	}, useCtx.InputsUsage())
}
