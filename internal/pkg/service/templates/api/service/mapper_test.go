package service

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
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

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
