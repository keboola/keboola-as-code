package use_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	. "github.com/keboola/keboola-as-code/internal/pkg/template/context/use"
	"github.com/keboola/keboola-as-code/internal/pkg/template/jsonnet/function"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func TestContext(t *testing.T) {
	t.Parallel()

	// Mocked ticket provider
	storageApiClient, httpTransport := client.NewMockedClient()
	tickets := storageapi.NewTicketProvider(context.Background(), storageApiClient)

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
	fs := aferofs.NewMemoryFs()

	// Enable inputUsageNotifier
	objectKey := model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", Id: "456"}
	fileDef := filesystem.NewFileDef("foo.bar")
	fileDef.AddMetadata(filesystem.ObjectKeyMetadata, objectKey)
	fileDef.AddTag(model.FileKindObjectConfig)
	ctx := context.WithValue(context.Background(), jsonnetfiles.FileDefCtxKey, fileDef)

	// Create template use context
	useCtx := NewContext(ctx, templateRef, fs, instanceId, targetBranch, inputsValues, map[string]*template.Input{}, tickets, testapi.MockedComponentsMap())

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
					JsonKey: orderedmap.PathFromStr("Input1"),
				},
				{
					Name:    "input-2",
					JsonKey: orderedmap.PathFromStr("Input2"),
				},
				{
					Name:    "input-3",
					JsonKey: orderedmap.PathFromStr("Input3"),
				},
				{
					Name:    "input-4",
					JsonKey: orderedmap.PathFromStr("Input4"),
				},
				// "input-5" IsSkipped, so it is not present here, it was not filled in by the user.
			},
		},
	}, useCtx.InputsUsage())
}

func TestComponentsFunctions(t *testing.T) {
	t.Parallel()

	// Mocked ticket provider
	storageApiClient, _ := client.NewMockedClient()
	tickets := storageapi.NewTicketProvider(context.Background(), storageApiClient)
	components := model.NewComponentsMap(storageapi.Components{})
	targetBranch := model.BranchKey{Id: 123}
	inputsValues := template.InputsValues{}
	inputs := map[string]*template.Input{}
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceId := "my-instance"
	fs := aferofs.NewMemoryFs()
	ctx := context.Background()

	// Context factory for template use operation
	newUseCtx := func() *Context {
		return NewContext(ctx, templateRef, fs, instanceId, targetBranch, inputsValues, inputs, tickets, components)
	}

	// Jsonnet template
	code := `
{
"keboola.wr-db-snowflake": ComponentIsAvailable("keboola.wr-db-snowflake"),
"keboola.wr-snowflake-blob-storage": ComponentIsAvailable("keboola.wr-snowflake-blob-storage"),
"wr-snowflake": SnowflakeWriterComponentId(),
}
`

	// Case 1: No component is defined
	output, err := jsonnet.Evaluate(code, newUseCtx().JsonNetContext())
	expected := ""
	assert.Error(t, err)
	assert.Equal(t, "jsonnet error: RUNTIME ERROR: no Snowflake Writer component found", err.Error())
	assert.Equal(t, expected, output)

	// Case 2: Only AWS Snowflake Writer
	components = model.NewComponentsMap(storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: function.SnowflakeWriterIDAws}},
	})
	expected = `
{
  "keboola.wr-db-snowflake": true,
  "keboola.wr-snowflake-blob-storage": false,
  "wr-snowflake": "keboola.wr-db-snowflake"
}
`
	output, err = jsonnet.Evaluate(code, newUseCtx().JsonNetContext())
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))

	// Case 3: Only Azure Snowflake Writer
	components = model.NewComponentsMap(storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: function.SnowflakeWriterIDAzure}},
	})
	expected = `
{
  "keboola.wr-db-snowflake": false,
  "keboola.wr-snowflake-blob-storage": true,
  "wr-snowflake": "keboola.wr-snowflake-blob-storage"
}
`
	output, err = jsonnet.Evaluate(code, newUseCtx().JsonNetContext())
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))

	// Case 4: Both AWS and Azure Snowflake Writer
	components = model.NewComponentsMap(storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: function.SnowflakeWriterIDAws}},
		{ComponentKey: storageapi.ComponentKey{ID: function.SnowflakeWriterIDAzure}},
	})
	expected = `
{
  "keboola.wr-db-snowflake": true,
  "keboola.wr-snowflake-blob-storage": true,
  "wr-snowflake": "keboola.wr-db-snowflake"
}
`
	output, err = jsonnet.Evaluate(code, newUseCtx().JsonNetContext())
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))
}
