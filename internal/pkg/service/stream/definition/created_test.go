package definition_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestCreated(t *testing.T) {
	t.Parallel()
}

func TestCreated_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[definition.Created]{
		{
			Name: "empty",
			ExpectedError: `
- "created.at" is a required field
- "created.by" is a required field
`,
			Value: definition.Created{},
		},
		{
			Name:  "ok",
			Value: test.Created(),
		},
	}

	// Run test cases
	cases.Run(t)
}
