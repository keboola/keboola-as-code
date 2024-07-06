package local_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestConfig_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[local.Config]{
		{
			Name: "empty",
			ExpectedError: `
- "volume.assignment.count" is a required field
- "volume.assignment.preferredTypes" is a required field
- "volume.registration.ttlSeconds" is a required field
- "volume.sync.mode" is a required field
- "volume.allocation.static" is a required field
- "volume.allocation.relative" must be 100 or greater
- "encoding.compression.type" is a required field
`,
			Value: local.Config{},
		},
		{
			Name:  "default",
			Value: local.NewConfig(),
		},
	}

	// Run test cases
	cases.Run(t)
}
