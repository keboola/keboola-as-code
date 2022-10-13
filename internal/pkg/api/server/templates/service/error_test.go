package service

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestValidationErrorFormatter(t *testing.T) {
	t.Parallel()

	errs := errors.NewMultiError()
	errs.Append(errors.New("My error!"))
	sub := errors.NewMultiError()
	sub.Append(errors.New("go lang error 1"))
	sub.Append(errors.New("go lang error 2"))
	errs.AppendWithPrefix(sub, "prefix")

	expected := `
- My error!
- Prefix:
  - Go lang error 1.
  - Go lang error 2.
`

	f := NewValidationErrorFormatter()
	assert.Equal(t, strings.TrimSpace(expected), f.Format(errs))
}
