package requestcookie_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/requestcookie"
)

func TestRemove(t *testing.T) {
	t.Parallel()

	drop := func(name string) bool { return name == "drop" }

	cases := []struct {
		name string
		in   []string
		out  []string
	}{
		{name: "no cookie header", in: nil, out: nil},
		{name: "nothing dropped keeps the line unchanged", in: []string{"a=1;b=2;  c=3"}, out: []string{"a=1;b=2;  c=3"}},
		{name: "empty parts and nothing dropped keep the line unchanged", in: []string{"a=1;", "a=1;;b=2"}, out: []string{"a=1;", "a=1;;b=2"}},
		{name: "dropped in the middle", in: []string{"a=1; drop=x; b=2"}, out: []string{"a=1; b=2"}},
		{name: "values with = and spaces are kept", in: []string{"drop=x; s=a=b=c; q=\"x y\""}, out: []string{"s=a=b=c; q=\"x y\""}},
		{name: "name with spaces around it", in: []string{"a=1;  drop =x"}, out: []string{"a=1"}},
		{name: "every occurrence", in: []string{"drop=1; a=1; drop=2"}, out: []string{"a=1"}},
		{name: "line emptied by the removal is dropped", in: []string{"drop=1", "a=1", "b=2;c=3"}, out: []string{"a=1", "b=2;c=3"}},
		{name: "all removed", in: []string{"drop=1", "drop=2"}, out: nil},
		{name: "similar names are kept", in: []string{"drop_0=1; xdrop=2; drop=3"}, out: []string{"drop_0=1; xdrop=2"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/", nil)
			require.NoError(t, err)
			for _, line := range tc.in {
				req.Header.Add("Cookie", line)
			}

			requestcookie.Remove(req, drop)

			assert.Equal(t, tc.out, req.Header.Values("Cookie"))
		})
	}
}
