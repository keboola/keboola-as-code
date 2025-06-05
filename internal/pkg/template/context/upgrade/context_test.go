package upgrade_test

import (
	"strings"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	dependenciesPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	. "github.com/keboola/keboola-as-code/internal/pkg/template/context/upgrade"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func TestContext(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Mocked ticket provider
	_, httpTransport := client.NewMockedClient()
	httpTransport.RegisterResponder(resty.MethodGet, `https://connection.keboola.com/v2/storage/?exclude=components`,
		httpmock.NewStringResponder(200, `{
			"services": [],
			"features": []
		}`),
	)

	// Inputs
	targetBranch := model.BranchKey{ID: 123}
	inputsValues := template.InputsValues{
		{
			ID:    "input-1",
			Value: "my-value-1",
		},
		{
			ID:    "input-2",
			Value: 789,
		},
		{
			ID:    "input-3",
			Value: 3.50,
		},
		{
			ID:    "input-4",
			Value: false,
		},
	}

	// Template
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceID := "my-instance"

	// Current project state
	d := dependenciesPkg.NewMocked(t, ctx)
	projectState := d.MockedState()
	configKey := model.ConfigKey{BranchID: targetBranch.ID, ComponentID: "foo.bar", ID: "12345"}
	rowKey := model.ConfigRowKey{BranchID: targetBranch.ID, ComponentID: "foo.bar", ConfigID: "12345", ID: "67890"}
	configMetadata := make(model.ConfigMetadata)
	configMetadata.SetTemplateInstance(templateRef.Repository().Name, templateRef.TemplateID(), instanceID)
	configMetadata.SetConfigTemplateID("my-config")
	configMetadata.AddRowTemplateID("67890", "my-row")
	require.NoError(t, projectState.Set(&model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey: configKey,
			Metadata:  configMetadata,
		},
	}))
	require.NoError(t, projectState.Set(&model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: rowKey},
		Local: &model.ConfigRow{
			ConfigRowKey: rowKey,
		},
	}))

	// Create context
	fs := aferofs.NewMemoryFs()
	tmplContext := NewContext(
		t.Context(),
		templateRef,
		fs,
		instanceID,
		targetBranch,
		inputsValues,
		map[string]*template.Input{},
		testapi.MockedComponentsMap(),
		projectState,
		d.ProjectBackends(),
		d.NewIDGenerator(),
	)

	// Check Jsonnet functions
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
	expectedJSON := `
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
	jsonOutput, err := jsonnet.Evaluate(code, tmplContext.JsonnetContext())
	require.NoError(t, err)
	assert.JSONEq(t, strings.TrimLeft(expectedJSON, "\n"), jsonOutput)

	// Check tickets replacement
	data := orderedmap.New()
	json.MustDecodeString(jsonOutput, data)
	replacements, err := tmplContext.Replacements()
	require.NoError(t, err)
	modifiedData, err := replacements.Replace(data)
	require.NoError(t, err)
	modifiedJSON := json.MustEncodeString(modifiedData, true)

	expectedJSON = `
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
	assert.JSONEq(t, strings.TrimLeft(expectedJSON, "\n"), modifiedJSON)
}
