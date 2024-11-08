package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestJobSchema(t *testing.T) {
	t.Parallel()
	s := forJob(serde.NewJSON(serde.NoValidation))

	jobKey := test.NewJobKey()
	cases := []struct{ actual, expected string }{
		{
			s.ForJob(jobKey).Key(),
			"storage/keboola/job/123/456/my-source/my-sink/1111",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}
