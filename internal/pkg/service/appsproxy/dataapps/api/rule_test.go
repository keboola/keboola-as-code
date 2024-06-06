package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRule_Match(t *testing.T) {
	t.Parallel()

	type testCase struct {
		Description   string
		Rule          Rule
		URL           string
		ExpectedMatch bool
		ExpectedErr   string
	}

	cases := []testCase{
		{
			Description:   `empty`,
			Rule:          Rule{},
			URL:           "https://test.com/foo",
			ExpectedMatch: false,
			ExpectedErr:   `unexpected data app auth rule ""`,
		},
		{
			Description:   `unexpected type`,
			Rule:          Rule{Type: "unknown"},
			URL:           "https://test.com/foo",
			ExpectedMatch: false,
			ExpectedErr:   `unexpected data app auth rule "unknown"`,
		},
		{
			Description:   `missing value`,
			Rule:          Rule{Type: RulePathPrefix},
			URL:           "https://test.com/",
			ExpectedMatch: false,
			ExpectedErr:   `rule "pathPrefix": value "" must start with "/"`,
		},
		{
			Description:   `"/" matches any request (1)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/"},
			URL:           "https://test.com/",
			ExpectedMatch: true,
		},
		{
			Description:   `"/" matches any request (2)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/"},
			URL:           "https://test.com/foo",
			ExpectedMatch: true,
		},
		{
			Description:   `"/{$}" matches only "/" (1)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/{$}"},
			URL:           "https://test.com/",
			ExpectedMatch: true,
		},
		{
			Description:   `"/{$}" matches only "/" (2)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/{$}"},
			URL:           "https://test.com/foo",
			ExpectedMatch: false,
		},
		{
			Description:   `"/foo{$}" matches only "/foo" (1)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/foo{$}"},
			URL:           "https://test.com/",
			ExpectedMatch: false,
		},
		{
			Description:   `"/foo{$}" matches only "/foo" (2)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/foo{$}"},
			URL:           "https://test.com/foo",
			ExpectedMatch: true,
		},
		{
			Description:   `"/foo{$}" matches only "/foo" (3)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/foo{$}"},
			URL:           "https://test.com/foo/bar",
			ExpectedMatch: false,
		},
		{
			Description:   `"/static/" matches request whose path begins with "/static/" (1)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/static/"},
			URL:           "https://test.com/",
			ExpectedMatch: false,
		},
		{
			Description:   `"/static/" matches request whose path begins with "/static/" (2)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/static/"},
			URL:           "https://test.com/static",
			ExpectedMatch: false,
		},
		{
			Description:   `"/static/" matches request whose path begins with "/static/" (3)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/static/"},
			URL:           "https://test.com/static/",
			ExpectedMatch: true,
		},
		{
			Description:   `"/static/" matches request whose path begins with "/static/" (4)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/static/"},
			URL:           "https://test.com/static/foo/bar",
			ExpectedMatch: true,
		},
		{
			Description:   `"/index.html" matches the path "/index.html" (1)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/index.html"},
			URL:           "https://test.com/",
			ExpectedMatch: false,
		},
		{
			Description:   `"/index.html" matches the path "/index.html" (2)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/index.html"},
			URL:           "https://test.com/index.html",
			ExpectedMatch: true,
		},
		{
			Description:   `"/index.html" matches the path "/index.html" (3)`,
			Rule:          Rule{Type: RulePathPrefix, Value: "/index.html"},
			URL:           "https://test.com/index.html.tmpl",
			ExpectedMatch: false,
		},
	}

	for _, tc := range cases {
		matched, err := tc.Rule.Match(httptest.NewRequest(http.MethodGet, tc.URL, nil))
		assert.Equal(t, tc.ExpectedMatch, matched, tc.Description)
		if tc.ExpectedErr == "" {
			require.NoError(t, err, tc.Description)
		} else if assert.Error(t, err, tc.Description) {
			assert.Equal(t, tc.ExpectedErr, err.Error())
		}
	}
}
