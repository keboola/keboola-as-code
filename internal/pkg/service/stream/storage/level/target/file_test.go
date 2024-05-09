package target

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestFile_Validation(t *testing.T) {
	cases := testvalidation.TestCases[Target]{
		{
			Name:  "ok",
			Value: Target{},
		},
	}

	cases.Run(t)
}
