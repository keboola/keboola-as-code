package definition

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type ValidationTestCase[T any] struct {
	Name          string
	ExpectedError string
	Value         T
}

type ValidationTestCases[T any] []ValidationTestCase[T]

func (v ValidationTestCases[T]) Run(t *testing.T) {
	ctx := context.Background()
	val := validator.New()
	for _, tc := range v {
		err := val.Validate(ctx, tc.Value)
		if tc.ExpectedError == "" {
			assert.NoError(t, err, tc.Name)
		} else {
			if assert.Error(t, err, tc.Name) {
				assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
			}
		}
	}
}
