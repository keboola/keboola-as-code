package upgrade_test

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

	dependenciesPkg "github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	. "github.com/keboola/keboola-as-code/internal/pkg/template/context/upgrade"
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
	}

	// Template
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceId := "my-instance"

	// Current project state
	d := dependenciesPkg.NewMockedDeps()
	projectState := d.MockedState()
	configKey := model.ConfigKey{BranchId: targetBranch.Id, ComponentId: "foo.bar", Id: "12345"}
	rowKey := model.ConfigRowKey{BranchId: targetBranch.Id, ComponentId: "foo.bar", ConfigId: "12345", Id: "67890"}
	configMetadata := make(model.ConfigMetadata)
	configMetadata.SetTemplateInstance(templateRef.Repository().Name, templateRef.TemplateId(), instanceId)
	configMetadata.SetConfigTemplateId("my-config")
	configMetadata.AddRowTemplateId("67890", "my-row")
	assert.NoError(t, projectState.Set(&model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey: configKey,
			Metadata:  configMetadata,
		},
	}))
	assert.NoError(t, projectState.Set(&model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: rowKey},
		Local: &model.ConfigRow{
			ConfigRowKey: rowKey,
		},
	}))

	// Create context
	fs := aferofs.NewMemoryFs()
	ctx := NewContext(context.Background(), templateRef, fs, instanceId, targetBranch, inputsValues, map[string]*template.Input{}, tickets, testapi.MockedComponentsMap(), projectState)

	// Check JsonNet functions
	code := `
{
	Input1: Input("input-1"),
    Input2: Input("input-2"),
    Input3: Input("input-3"),
    Input4: Input("input-4"),
    Objects: {
      Config1: ConfigId("my-config"),
      Config2: ConfigId("my-config"),
      Config3: ConfigId("new-config"),
      Row1: ConfigRowId("my-row"),
      Row2: ConfigRowId("my-row"),
      Row3: ConfigRowId("new-row"),
    },
}
`
	expectedJson := `
{
  "Input1": "my-value-1",
  "Input2": 789,
  "Input3": 3.5,
  "Input4": false,
  "Objects": {
    "Config1": "<<~~placeholder:1~~>>",
    "Config2": "<<~~placeholder:1~~>>",
    "Config3": "<<~~placeholder:3~~>>",
    "Row1": "<<~~placeholder:2~~>>",
    "Row2": "<<~~placeholder:2~~>>",
    "Row3": "<<~~placeholder:4~~>>"
  }
}
`
	jsonOutput, err := jsonnet.Evaluate(code, ctx.JsonNetContext())
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expectedJson, "\n"), jsonOutput)

	// Check tickets replacement
	data := orderedmap.New()
	json.MustDecodeString(jsonOutput, data)
	replacements, err := ctx.Replacements()
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
  "Objects": {
    "Config1": "12345",
    "Config2": "12345",
    "Config3": "1001",
    "Row1": "67890",
    "Row2": "67890",
    "Row3": "1002"
  }
}
`
	assert.Equal(t, strings.TrimLeft(expectedJson, "\n"), modifiedJson)
}
