package definition

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test/testvalidation"
)

func TestBranch_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[Branch]{
		{
			Name: "empty",
			Value: Branch{
				BranchKey: key.BranchKey{
					ProjectID: 123,
					BranchID:  456,
				},
			},
		},
	}

	// Run test cases
	cases.Run(t)
}
