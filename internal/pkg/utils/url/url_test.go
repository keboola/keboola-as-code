package url

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseQuery(t *testing.T) {
	t.Parallel()

	res, err := ParseQuery("one=two&three=four&five=&six&seven[0]=eight&seven[1]=nine&ten[]=eleven&ten[]=twelve")
	require.NoError(t, err)

	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	exp.Set("five", "")
	exp.Set("six", "")
	exp.Set("seven", []any{"eight", "nine"})
	exp.Set("ten", []any{"eleven", "twelve"})
	assert.Equal(t, exp, res)
}

func TestParseQuery_Map(t *testing.T) {
	t.Parallel()

	res, err := ParseQuery("one[two]=three&one[four]=five")
	require.NoError(t, err)

	exp := orderedmap.New()
	exp.Set("one", orderedmap.FromPairs(
		[]orderedmap.Pair{
			{
				Key:   "two",
				Value: "three",
			},
			{
				Key:   "four",
				Value: "five",
			},
		},
	))
	assert.Equal(t, exp, res)
}

func TestParseQuery_Nested(t *testing.T) {
	t.Parallel()

	res, err := ParseQuery("k[x][0]=zero&k[x][2]=one&k[y][0]=two")
	require.NoError(t, err)

	exp := orderedmap.New()
	exp.Set("k", orderedmap.FromPairs(
		[]orderedmap.Pair{
			{
				Key:   "x",
				Value: []any{"zero", nil, "one"},
			},
			{
				Key:   "y",
				Value: []any{"two"},
			},
		},
	))
	assert.Equal(t, exp, res)
}

func TestSanitizeURLString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		inputURL string
		expected string
	}{
		{
			name:     "url with username and password",
			inputURL: "https://user:password@example.com/path?query=value",
			expected: "https://example.com/path?query=value",
		},
		{
			name:     "url with username only",
			inputURL: "ftp://user@example.com/resource",
			expected: "ftp://example.com/resource",
		},
		{
			name:     "url with no userinfo",
			inputURL: "http://example.com/page",
			expected: "http://example.com/page",
		},
		{
			name:     "malformed url",
			inputURL: "://example.com",
			expected: "://example.com",
		},
		{
			name:     "empty string",
			inputURL: "",
			expected: "",
		},
		{
			name:     "url with only host",
			inputURL: "example.com",
			expected: "example.com", // url.Parse will treat this as a path if no scheme
		},
		{
			name:     "url with scheme and host, no userinfo",
			inputURL: "https://example.com",
			expected: "https://example.com",
		},
		{
			name:     "url with special characters in userinfo",
			inputURL: "https://user%20name:p@$$wOrd@example.com",
			expected: "https://example.com",
		},
		{
			name:     "url with empty password",
			inputURL: "https://user:@example.com",
			expected: "https://example.com",
		},
		{
			name:     "url with empty username",
			inputURL: "https://:password@example.com",
			expected: "https://example.com",
		},
		{
			name:     "url with empty userinfo",
			inputURL: "https://@example.com",
			expected: "https://example.com", // No actual user info to sanitize
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, SanitizeURLString(tc.inputURL))
		})
	}
}
