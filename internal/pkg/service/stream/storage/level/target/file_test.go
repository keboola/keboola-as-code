package target

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestFile_Validation(t *testing.T) {
	cases := testvalidation.TestCases[Target]{
		{
			Name:          "empty",
			ExpectedError: `"provider" is a required field`,
			Value:         Target{},
		},
		{
			Name: "ok",
			Value: Target{
				Provider: "foo",
			},
		},
	}

	cases.Run(t)
}
