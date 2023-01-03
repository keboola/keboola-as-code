package receive

import (
	"io"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
)

func TestReadBody_TooLarge(t *testing.T) {
	t.Parallel()

	size := int(datasize.MB + 1)
	assert.Equal(t, 1024*1024+1, size)
	_, err := ReadBody(io.NopCloser(strings.NewReader(idgenerator.Random(size))))
	assert.EqualError(t, err, "payload too large, the maximum size is 1MB")
}
