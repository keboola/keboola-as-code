package service

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template/jsonnet/function"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

func TestTemplatesResponse(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		projectFeatures   keboola.Features
		stackComponents   keboola.Components
		expectedTemplates []string
	}

	tokenTemplate := keboola.Token{
		ID:       "token-12345-id",
		Token:    "my-secret",
		IsMaster: true,
		Owner: keboola.TokenOwner{
			ID:       12345,
			Name:     "Project 12345",
			Features: keboola.Features{},
		},
	}

	cases := []testCase{
		{
			name:              "default",
			projectFeatures:   nil,
			stackComponents:   nil,
			expectedTemplates: []string{"template1"},
		},
		{
			name:              "required-feature",
			projectFeatures:   keboola.Features{"my-feature"},
			stackComponents:   nil,
			expectedTemplates: []string{"template1", "template2-required-feature"},
		},
		{
			name:              "required-component",
			projectFeatures:   nil,
			stackComponents:   keboola.Components{{ComponentKey: keboola.ComponentKey{ID: "my.component"}, Name: "My Component"}},
			expectedTemplates: []string{"template1", "template3-required-component"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			// Mock features and components
			token := tokenTemplate
			token.Owner.Features = tc.projectFeatures
			d, _ := dependencies.NewMockedProjectRequestScope(
				t,
				ctx,
				config.New(),
				commonDeps.WithMockedStorageAPIToken(token),
				commonDeps.WithMockedComponents(tc.stackComponents),
			)

			// Create service
			svc, err := New(ctx, d)
			require.NoError(t, err)

			// List templates
			response, err := svc.TemplatesIndex(ctx, d, &templates.TemplatesIndexPayload{Repository: "keboola"})
			require.NoError(t, err)

			// Get templates IDs
			var actualIDs []string
			for _, template := range response.Templates {
				actualIDs = append(actualIDs, template.ID)
			}
			assert.Equal(t, tc.expectedTemplates, actualIDs)
		})
	}
}

func TestComponentsResponse(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		inputComponents []string
		projectBackends commonDeps.MockedOption
		mockComponents  keboola.Components
		expectedOutput  []string
	}

	cases := []testCase{
		{
			name:            "placeholder component",
			inputComponents: []string{manifest.SnowflakeWriterComponentIDPlaceholder},
			projectBackends: commonDeps.WithSnowflakeBackend(),
			mockComponents:  keboola.Components{{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDAws}}},
			expectedOutput:  []string{function.SnowflakeWriterIDAws.String()},
		},
		{
			name:            "placeholder component not found",
			inputComponents: []string{manifest.SnowflakeWriterComponentIDPlaceholder},
			projectBackends: commonDeps.WithSnowflakeBackend(),
			mockComponents:  keboola.Components{},
			expectedOutput:  []string{},
		},
		{
			name:            "multiple components with placeholders",
			inputComponents: []string{"component1", manifest.SnowflakeWriterComponentIDPlaceholder, "component2"},
			projectBackends: commonDeps.WithSnowflakeBackend(),
			mockComponents: keboola.Components{
				{ComponentKey: keboola.ComponentKey{ID: "component1"}},
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDAws}},
				{ComponentKey: keboola.ComponentKey{ID: "component2"}},
			},
			expectedOutput: []string{"component1", function.SnowflakeWriterIDAws.String(), "component2"},
		},
		{
			name:            "azure component",
			inputComponents: []string{manifest.SnowflakeWriterComponentIDPlaceholder},
			projectBackends: commonDeps.WithSnowflakeBackend(),
			mockComponents:  keboola.Components{{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDAzure}}},
			expectedOutput:  []string{function.SnowflakeWriterIDAzure.String()},
		},
		{
			name:            "gcp component with snowflake backend",
			inputComponents: []string{manifest.SnowflakeWriterComponentIDPlaceholder},
			projectBackends: commonDeps.WithSnowflakeBackend(),
			mockComponents: keboola.Components{
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCPS3}},
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCP}},
			},
			expectedOutput: []string{function.SnowflakeWriterIDGCPS3.String()},
		},
		{
			name:            "gcp component with bigquery backend",
			inputComponents: []string{manifest.SnowflakeWriterComponentIDPlaceholder},
			projectBackends: commonDeps.WithBigQueryBackend(),
			mockComponents: keboola.Components{
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCPS3}},
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCP}},
			},
			expectedOutput: []string{function.SnowflakeWriterIDGCP.String()},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			d, _ := dependencies.NewMockedProjectRequestScope(
				t,
				ctx,
				config.New(),
				commonDeps.WithMockedComponents(tc.mockComponents),
				tc.projectBackends,
			)

			actualOutput := ComponentsResponse(d, tc.inputComponents)

			assert.Equal(t, tc.expectedOutput, actualOutput)
		})
	}
}

func TestSnowflakePlaceholders(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		mockComponents  keboola.Components
		projectBackends commonDeps.MockedOption
		input           string
		expectedOutput  string
	}

	cases := []testCase{
		{
			name:            "Azure-component",
			mockComponents:  keboola.Components{{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDAzure}}},
			projectBackends: commonDeps.WithSnowflakeBackend(),
			input:           manifest.SnowflakeWriterComponentIDPlaceholder,
			expectedOutput:  function.SnowflakeWriterIDAzure.String(),
		},
		{
			name: "GCP-S3-component-with-Snowflake-backend",
			mockComponents: keboola.Components{
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCP}},
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCPS3}},
			},
			projectBackends: commonDeps.WithSnowflakeBackend(),
			input:           manifest.SnowflakeWriterComponentIDPlaceholder,
			expectedOutput:  function.SnowflakeWriterIDGCPS3.String(),
		},
		{
			name:            "GCP-component-with-BigQuery-backend",
			mockComponents:  keboola.Components{{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCP}}},
			projectBackends: commonDeps.WithBigQueryBackend(),
			input:           manifest.SnowflakeWriterComponentIDPlaceholder,
			expectedOutput:  function.SnowflakeWriterIDGCP.String(),
		},
		{
			name: "GCP-component-with-BigQuery-backend-icon",
			mockComponents: keboola.Components{
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCPS3}},
				{ComponentKey: keboola.ComponentKey{ID: function.SnowflakeWriterIDGCP}},
			},
			projectBackends: commonDeps.WithBigQueryBackend(),
			input:           "component:" + manifest.SnowflakeWriterComponentIDPlaceholder,
			expectedOutput:  "component:" + function.SnowflakeWriterIDGCP.String(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			d, _ := dependencies.NewMockedProjectRequestScope(
				t,
				ctx,
				config.New(),
				commonDeps.WithMockedComponents(tc.mockComponents),
				tc.projectBackends,
			)

			actualOutput := ReplacePlaceholders(d, tc.input)
			assert.Equal(t, tc.expectedOutput, actualOutput)
		})
	}
}
