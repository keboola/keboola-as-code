package definition_test

import (
	"strings"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestHTTPSource_Validation(t *testing.T) {
	t.Parallel()

	sourceKey := key.SourceKey{
		BranchKey: key.BranchKey{ProjectID: 123, BranchID: 456},
		SourceID:  "my-source",
	}

	// Test cases
	cases := testvalidation.TestCases[definition.Source]{
		{
			Name: "empty",
			ExpectedError: `
- "projectId" is a required field
- "branchId" is a required field
- "sourceId" is a required field
- "created.at" is a required field
- "created.by" is a required field
- "version.number" is a required field
- "version.hash" is a required field
- "version.at" is a required field
- "version.by" is a required field
- "type" is a required field
- "name" is a required field
`,
			Value: definition.Source{},
		},
		{
			Name:          "nil HTTP section",
			ExpectedError: `"http" is a required field`,
			Value: definition.Source{
				SourceKey:     sourceKey,
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeHTTP,
				Name:          "My Source",
				Description:   "My Description",
			},
		},
		{
			Name:          "empty HTTP section",
			ExpectedError: `"http.secret" is a required field`,
			Value: definition.Source{
				SourceKey:     sourceKey,
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeHTTP,
				Name:          "My Source",
				Description:   "My Description",
				HTTP:          &definition.HTTPSource{},
			},
		},
		{
			Name:          "short secret",
			ExpectedError: `"http.secret" must be 48 characters in length`,
			Value: definition.Source{
				SourceKey:     sourceKey,
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeHTTP,
				Name:          "My Source",
				Description:   "My Description",
				HTTP: &definition.HTTPSource{
					Secret: "foo",
				},
			},
		},
		{
			Name:          "long name",
			ExpectedError: `"name" must be a maximum of 40 characters in length`,
			Value: definition.Source{
				SourceKey:     sourceKey,
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeHTTP,
				Name:          strings.Repeat("a", 40+1),
				Description:   "My Description",
				HTTP: &definition.HTTPSource{
					Secret: "012345678901234567890123456789012345678912345678",
				},
			},
		},
		{
			Name:          "long description",
			ExpectedError: `"name" must be a maximum of 40 characters in length`,
			Value: definition.Source{
				SourceKey:     sourceKey,
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeHTTP,
				Name:          strings.Repeat("a", 4096+1),
				Description:   "My Description",
				HTTP: &definition.HTTPSource{
					Secret: "012345678901234567890123456789012345678912345678",
				},
			},
		},
		{
			Name: "ok",
			Value: definition.Source{
				SourceKey:     sourceKey,
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeHTTP,
				Name:          "My Source",
				Description:   "My Description",
				HTTP: &definition.HTTPSource{
					Secret: "012345678901234567890123456789012345678912345678",
				},
			},
		},
	}

	// Run test cases
	cases.Run(t)
}
