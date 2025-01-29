package use_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	dependenciesPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	. "github.com/keboola/keboola-as-code/internal/pkg/template/context/use"
	"github.com/keboola/keboola-as-code/internal/pkg/template/jsonnet/function"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func TestContext(t *testing.T) {
	t.Parallel()

	// Mocked ticket provider
	c, httpTransport := client.NewMockedClient()
	httpTransport.RegisterResponder(resty.MethodGet, `/v2/storage/?exclude=components`,
		httpmock.NewStringResponder(200, `{
			"services": [],
			"features": []
		}`),
	)
	ctx := context.Background()
	api, err := keboola.NewAuthorizedAPI(ctx, "https://connection.keboola.com", "my-token", keboola.WithClient(&c))
	require.NoError(t, err)
	tickets := keboola.NewTicketProvider(ctx, api)

	// Mocked tickets
	var ticketResponses []*http.Response
	for i := 1; i <= 2; i++ {
		response, err := httpmock.NewJsonResponse(200, map[string]any{"id": cast.ToString(1000 + i)})
		require.NoError(t, err)
		ticketResponses = append(ticketResponses, response)
	}
	httpTransport.RegisterResponder("POST", `=~/storage/tickets`, httpmock.ResponderFromMultipleResponses(ticketResponses))

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
		{
			ID:      "input-5",
			Value:   "",
			Skipped: true,
		},
	}

	// Create context
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceID := "my-instance"
	fs := aferofs.NewMemoryFs()

	// Enable inputUsageNotifier
	objectKey := model.ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "456"}
	fileDef := filesystem.NewFileDef("foo.bar")
	fileDef.AddMetadata(filesystem.ObjectKeyMetadata, objectKey)
	fileDef.AddTag(model.FileKindObjectConfig)
	ctxWithVal := context.WithValue(context.Background(), jsonnetfiles.FileDefCtxKey, fileDef)

	// Create template use context
	d := dependenciesPkg.NewMocked(t, ctx)
	projectState := d.MockedState()
	useCtx := NewContext(ctxWithVal, templateRef, fs, instanceID, targetBranch, inputsValues, map[string]*template.Input{}, tickets, testapi.MockedComponentsMap(), projectState, d.ProjectBackends())

	// Check Jsonnet functions
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
	expectedJSON := `
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
	jsonOutput, err := jsonnet.Evaluate(code, useCtx.JsonnetContext())
	require.NoError(t, err)
	assert.JSONEq(t, strings.TrimLeft(expectedJSON, "\n"), jsonOutput)

	// Check tickets replacement
	data := orderedmap.New()
	json.MustDecodeString(jsonOutput, data)
	replacements, err := useCtx.Replacements()
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
  "Input5": "",
  "Objects": {
    "Config1": "1001",
    "Config2": "1001",
    "Row1": "1002",
    "Row2": "1002"
  }
}
`
	assert.JSONEq(t, strings.TrimLeft(expectedJSON, "\n"), modifiedJSON)

	// Check collected inputs usage
	assert.Equal(t, &metadata.InputsUsage{
		Values: metadata.InputsUsageMap{
			objectKey: []metadata.InputUsage{
				{
					Name:    "input-1",
					JSONKey: orderedmap.PathFromStr("Input1"),
				},
				{
					Name:    "input-2",
					JSONKey: orderedmap.PathFromStr("Input2"),
				},
				{
					Name:    "input-3",
					JSONKey: orderedmap.PathFromStr("Input3"),
				},
				{
					Name:    "input-4",
					JSONKey: orderedmap.PathFromStr("Input4"),
				},
				// "input-5" IsSkipped, so it is not present here, it was not filled in by the user.
			},
		},
	}, useCtx.InputsUsage())
}

func TestComponentsFunctions(t *testing.T) {
	t.Parallel()

	// Mocked ticket provider
	c, httpTransport := client.NewMockedClient()
	httpTransport.RegisterResponder(resty.MethodGet, `/v2/storage/?exclude=components`,
		httpmock.NewStringResponder(200, `{
			"services": [],
			"features": []
		}`),
	)
	ctx := context.Background()
	api, err := keboola.NewAuthorizedAPI(ctx, "https://connection.keboola.com", "my-token", keboola.WithClient(&c))
	require.NoError(t, err)

	d := dependenciesPkg.NewMocked(t, ctx, dependenciesPkg.WithSnowflakeBackend())
	projectState := d.MockedState()
	tickets := keboola.NewTicketProvider(context.Background(), api)
	components := model.NewComponentsMap(keboola.Components{})
	targetBranch := model.BranchKey{ID: 123}
	inputsValues := template.InputsValues{}
	inputs := map[string]*template.Input{}
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceID := "my-instance"
	fs := aferofs.NewMemoryFs()

	// Context factory for template use operation
	newUseCtx := func() *Context {
		return NewContext(ctx, templateRef, fs, instanceID, targetBranch, inputsValues, inputs, tickets, components, projectState, d.ProjectBackends())
	}

	// Jsonnet template
	code := `
{
"keboola.wr-db-snowflake": ComponentIsAvailable("keboola.wr-db-snowflake"),
"keboola.wr-db-snowflake-gcs-s3": ComponentIsAvailable("keboola.wr-db-snowflake-gcs-s3"),
"keboola.wr-snowflake-blob-storage": ComponentIsAvailable("keboola.wr-snowflake-blob-storage"),
"wr-snowflake": SnowflakeWriterComponentId(),
}
`

	// Case 1: No component is defined
	output, err := jsonnet.Evaluate(code, newUseCtx().JsonnetContext())
	expected := ""
	require.Error(t, err)
	assert.Equal(t, "jsonnet error: RUNTIME ERROR: no Snowflake Writer component found", err.Error())
	assert.Equal(t, expected, output)

	// Case 2: Only AWS Snowflake Writer
	components = model.NewComponentsMap(keboola.Components{
		{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDAws}},
	})
	expected = `
{
  "keboola.wr-db-snowflake": true,
  "keboola.wr-db-snowflake-gcs-s3": false,
  "keboola.wr-snowflake-blob-storage": false,
  "wr-snowflake": "keboola.wr-db-snowflake"
}
`
	output, err = jsonnet.Evaluate(code, newUseCtx().JsonnetContext())
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))

	// Case 3: Only Azure Snowflake Writer
	components = model.NewComponentsMap(keboola.Components{
		{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDAzure}},
	})
	expected = `
{
  "keboola.wr-db-snowflake": false,
  "keboola.wr-db-snowflake-gcs-s3": false,
  "keboola.wr-snowflake-blob-storage": true,
  "wr-snowflake": "keboola.wr-snowflake-blob-storage"
}
`
	output, err = jsonnet.Evaluate(code, newUseCtx().JsonnetContext())
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))

	// Case 4: Only Google Snowflake Writer
	components = model.NewComponentsMap(keboola.Components{
		{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCP}},
		{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCPS3}},
	})
	expected = `
{
  "keboola.wr-db-snowflake": false,
  "keboola.wr-db-snowflake-gcs-s3": true,
  "keboola.wr-snowflake-blob-storage": false,
  "wr-snowflake": "keboola.wr-db-snowflake-gcs-s3"
}
`
	output, err = jsonnet.Evaluate(code, newUseCtx().JsonnetContext())
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))

	// Case 5: Both AWS and Azure Snowflake Writer
	components = model.NewComponentsMap(keboola.Components{
		{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDAws}},
		{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDAzure}},
	})
	expected = `
{
  "keboola.wr-db-snowflake": true,
  "keboola.wr-db-snowflake-gcs-s3": false,
  "keboola.wr-snowflake-blob-storage": true,
  "wr-snowflake": "keboola.wr-db-snowflake"
}
`
	output, err = jsonnet.Evaluate(code, newUseCtx().JsonnetContext())
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))
}

func TestHasBackendFunction(t *testing.T) {
	t.Parallel()

	// Mocked ticket provider
	c, httpTransport := client.NewMockedClient()
	httpTransport.RegisterResponder(resty.MethodGet, `/v2/storage/?exclude=components`,
		httpmock.NewStringResponder(200, `{
			"services": [],
			"features": []
		}`),
	)
	ctx := context.Background()

	d := dependenciesPkg.NewMocked(t, ctx, dependenciesPkg.WithSnowflakeBackend())

	api, err := keboola.NewAuthorizedAPI(ctx, "https://connection.keboola.com", d.StorageAPIToken().Token, keboola.WithClient(&c))
	require.NoError(t, err)

	projectState := d.MockedState()
	tickets := keboola.NewTicketProvider(context.Background(), api)
	components := model.NewComponentsMap(keboola.Components{})
	targetBranch := model.BranchKey{ID: 123}
	inputsValues := template.InputsValues{}
	inputs := map[string]*template.Input{}
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceID := "my-instance"
	fs := aferofs.NewMemoryFs()

	// Context factory for template use operation
	newUseCtx := func() *Context {
		return NewContext(ctx, templateRef, fs, instanceID, targetBranch, inputsValues, inputs, tickets, components, projectState, d.ProjectBackends())
	}

	// Jsonnet template
	code := `
{
	"snowflake": HasProjectBackend('snowflake'),
	"bigquery": HasProjectBackend('bigquery')
}
`
	// Case 1: project backend 'snowflake'
	expected := `
{
  "bigquery": false,
  "snowflake": true
}
`

	output, err := jsonnet.Evaluate(code, newUseCtx().JsonnetContext())
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))

	// Case 2 backend 'bigquery'
	d = dependenciesPkg.NewMocked(t, ctx, dependenciesPkg.WithBigQueryBackend())

	expected = `
{
  "bigquery": true,
  "snowflake": false
}
`
	output, err = jsonnet.Evaluate(code, newUseCtx().JsonnetContext())
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(output))
}
