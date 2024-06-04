package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestFileSchema(t *testing.T) {
	t.Parallel()
	s := forFile(serde.NewJSON(serde.NoValidation))

	fileKey := test.NewFileKey()

	cases := []struct{ actual, expected string }{
		{
			s.ForFile(fileKey).Key(),
			"storage/keboola/file/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, `case "%d"`, i+1)
	}
}
