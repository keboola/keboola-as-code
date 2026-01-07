package export

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
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

// mockPrompt implements prompt.Prompt for testing.
type mockPrompt struct {
	confirmResult bool
}

func (m *mockPrompt) IsInteractive() bool                                { return true }
func (m *mockPrompt) Printf(_ string, _ ...any)                          {}
func (m *mockPrompt) Confirm(_ *prompt.Confirm) bool                     { return m.confirmResult }
func (m *mockPrompt) Ask(q *prompt.Question) (string, bool)              { return q.Default, true }
func (m *mockPrompt) Select(s *prompt.Select) (string, bool)             { return s.Default, true }
func (m *mockPrompt) SelectIndex(s *prompt.SelectIndex) (int, bool)      { return s.Default, true }
func (m *mockPrompt) MultiSelect(s *prompt.MultiSelect) ([]string, bool) { return s.Default, true }
func (m *mockPrompt) MultiSelectIndex(s *prompt.MultiSelectIndex) ([]int, bool) {
	return s.Default, true
}
func (m *mockPrompt) Multiline(q *prompt.Question) (string, bool)        { return q.Default, true }
func (m *mockPrompt) Editor(_ string, q *prompt.Question) (string, bool) { return q.Default, true }

// mockValidateDeps implements validateDependencies for testing.
type mockValidateDeps struct {
	fs      filesystem.Fs
	dialogs *dialog.Dialogs
}

func (m *mockValidateDeps) Fs() filesystem.Fs        { return m.fs }
func (m *mockValidateDeps) Dialogs() *dialog.Dialogs { return m.dialogs }

func TestValidateDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		files         []string
		dirs          []string
		force         bool
		confirmResult bool
		expectError   bool
		errorContains string
	}{
		{
			name:        "empty directory",
			files:       nil,
			dirs:        nil,
			force:       false,
			expectError: false,
		},
		{
			name:        "only allowed files",
			files:       []string{".env", ".env.local", ".gitignore"},
			dirs:        []string{".keboola", ".git"},
			force:       false,
			expectError: false,
		},
		{
			name:        "conflicts with force flag",
			files:       []string{"README.md", "src"},
			dirs:        nil,
			force:       true,
			expectError: false,
		},
		{
			name:          "conflicts without force, user confirms",
			files:         []string{"README.md"},
			dirs:          nil,
			force:         false,
			confirmResult: true,
			expectError:   false,
		},
		{
			name:          "conflicts without force, user rejects",
			files:         []string{"README.md"},
			dirs:          nil,
			force:         false,
			confirmResult: false,
			expectError:   true,
			errorContains: "export cancelled by user",
		},
		{
			name:        "mixed allowed and not allowed files with force",
			files:       []string{".env", ".gitignore", "main.go", "go.mod"},
			dirs:        []string{".keboola"},
			force:       true,
			expectError: false,
		},
		{
			name:          "mixed files without force, user rejects",
			files:         []string{".env", "main.go"},
			dirs:          nil,
			force:         false,
			confirmResult: false,
			expectError:   true,
			errorContains: "export cancelled by user",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			// Create memory filesystem
			fs := aferofs.NewMemoryFs()

			// Create test files
			for _, f := range tc.files {
				require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(f, "test content")))
			}

			// Create test directories
			for _, d := range tc.dirs {
				require.NoError(t, fs.Mkdir(ctx, d))
			}

			// Create mock dependencies
			mockP := &mockPrompt{confirmResult: tc.confirmResult}
			deps := &mockValidateDeps{
				fs:      fs,
				dialogs: dialog.New(mockP),
			}

			// Create flags
			flags := Flags{
				Force: configmap.Value[bool]{Value: tc.force},
			}

			// Run validation
			err := validateDirectory(ctx, deps, flags)

			// Check result
			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
