package service

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
)

func Test_getTemplateVersion_Requirements(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		templateID      string
		projectFeatures keboola.Features
		stackComponents keboola.Components
		exceptedError   error
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
			name:            "default",
			templateID:      "template1",
			projectFeatures: nil,
			stackComponents: nil,
			exceptedError:   nil,
		},
		{
			name:            "wrong-required-feature",
			templateID:      "template2-required-feature",
			projectFeatures: keboola.Features{"my-feature-wrong"},
			stackComponents: nil,
			exceptedError:   &templates.GenericError{Name: "templates.templateNoRequirements", Message: `Template "template2-required-feature" doesn't have requirements.`},
		},
		{
			name:            "required-feature",
			templateID:      "template2-required-feature",
			projectFeatures: keboola.Features{"my-feature"},
			stackComponents: nil,
			exceptedError:   nil,
		},
		{
			name:            "required-component",
			templateID:      "template3-required-component",
			projectFeatures: nil,
			stackComponents: keboola.Components{{ComponentKey: keboola.ComponentKey{ID: "my.component"}, Name: "My Component"}},
			exceptedError:   nil,
		},
		{
			name:            "wrong-required-component",
			templateID:      "template3-required-component",
			projectFeatures: nil,
			stackComponents: keboola.Components{{ComponentKey: keboola.ComponentKey{ID: "my.component.wrong"}, Name: "My Component-wrong"}},
			exceptedError:   &templates.GenericError{Name: "templates.templateNoRequirements", Message: `Template "template3-required-component" doesn't have requirements.`},
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
				commonDeps.WithMockedFeatures(tc.projectFeatures),
			)

			_, _, err := getTemplateVersion(ctx, d, "keboola", tc.templateID, "default")
			assert.Equal(t, err, tc.exceptedError)
		})
	}
}
