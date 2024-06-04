package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestTokenSchema(t *testing.T) {
	t.Parallel()
	s := forToken(serde.NewJSON(serde.NoValidation))

	sinkKey := test.NewSinkKey()

	cases := []struct{ actual, expected string }{
		{
			s.ForSink(sinkKey).Key(),
			"storage/keboola/secret/token/123/456/my-source/my-sink",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, `case "%d"`, i+1)
	}
}
