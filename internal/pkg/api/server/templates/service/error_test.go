package service

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestValidationErrorFormatter(t *testing.T) {
	t.Parallel()

	errs := utils.NewMultiError()
	errs.Append(fmt.Errorf("My error!"))
	sub := utils.NewMultiError()
	sub.Append(fmt.Errorf("go lang error 1"))
	sub.Append(fmt.Errorf("go lang error 2"))
	errs.AppendWithPrefix("prefix", sub)

	expected := `
- My error!
- Prefix:
  - Go lang error 1.
  - Go lang error 2.
`

	f := NewValidationErrorFormatter()
	assert.Equal(t, strings.TrimSpace(expected), f.Format(errs))
}
