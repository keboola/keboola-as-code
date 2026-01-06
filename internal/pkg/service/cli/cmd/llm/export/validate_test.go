package export

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsAllowedFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		// Allowed files
		{name: "keboola dir", filename: ".keboola", expected: true},
		{name: "env file", filename: ".env", expected: true},
		{name: "env local", filename: ".env.local", expected: true},
		{name: "env dist", filename: ".env.dist", expected: true},
		{name: "env custom", filename: ".env.production", expected: true},
		{name: "gitignore", filename: ".gitignore", expected: true},
		{name: "git dir", filename: ".git", expected: true},

		// Not allowed files
		{name: "readme", filename: "README.md", expected: false},
		{name: "src dir", filename: "src", expected: false},
		{name: "go mod", filename: "go.mod", expected: false},
		{name: "twin format", filename: "twin_format", expected: false},
		{name: "main dir", filename: "main", expected: false},
		{name: "hidden file", filename: ".hidden", expected: false},
		{name: "env without dot prefix", filename: "env", expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := isAllowedFile(tc.filename)
			assert.Equal(t, tc.expected, result, "isAllowedFile(%q) should be %v", tc.filename, tc.expected)
		})
	}
}
