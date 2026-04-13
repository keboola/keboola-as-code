package dbtutil_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt/dbtutil"
)

func TestBaseURLFromHost(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{"https://connection.keboola.com", "https://query.keboola.com"},
		{"https://connection.eu-west-1.keboola.com", "https://query.eu-west-1.keboola.com"},
		{"http://connection.keboola.com", "http://query.keboola.com"},
		{"connection.keboola.com", "https://query.keboola.com"},
		{"https://my-stack.keboola.com", "https://query.my-stack.keboola.com"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, dbtutil.BaseURLFromHost(tc.in))
		})
	}
}
