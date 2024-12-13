//nolint:forbidigo
package testhelper_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	. "github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type mockedT struct {
	buf *bytes.Buffer
}

// Implements TestingT for mockedT.
func (t *mockedT) Errorf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	t.buf.WriteString(s)
}

func TestAssertDirectoryFileOnlyInExpected(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create file
	require.NoError(t, expectedFs.WriteFile(context.Background(), filesystem.NewRawFile("file.txt", "foo\n")))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "only in expected \".+file.txt\"", test.buf.String())
}

func TestAssertDirectoryDirOnlyInExpected(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create directory
	require.NoError(t, expectedFs.Mkdir(context.Background(), `myDir`))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "only in expected \".+myDir\"", test.buf.String())
}

func TestAssertDirectoryFileOnlyInActual(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create file
	require.NoError(t, actualFs.WriteFile(context.Background(), filesystem.NewRawFile("file.txt", "foo\n")))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "only in actual \".+file.txt\"", test.buf.String())
}

func TestAssertDirectoryDirOnlyInActual(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create directory
	require.NoError(t, actualFs.Mkdir(context.Background(), `myDir`))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "only in actual \".+myDir\"", test.buf.String())
}

func TestAssertDirectoryFileDifferentType1(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create file in actual
	require.NoError(t, actualFs.WriteFile(context.Background(), filesystem.NewRawFile("myNode", "foo\n")))

	// Create directory in expected
	require.NoError(t, expectedFs.Mkdir(context.Background(), `myNode`))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "\"myNode\" is file in actual, but dir in expected")
}

func TestAssertDirectoryFileDifferentType2(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create file in expected
	require.NoError(t, expectedFs.WriteFile(context.Background(), filesystem.NewRawFile("myNode", "foo\n")))

	// Create directory in actual
	require.NoError(t, actualFs.Mkdir(context.Background(), `myNode`))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "\"myNode\" is dir in actual, but file in expected")
}

func TestAssertDirectoryDifferentContent(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	require.NoError(t, expectedFs.WriteFile(context.Background(), filesystem.NewRawFile("file.txt", "foo\n")))

	// File in actual - different content
	require.NoError(t, actualFs.WriteFile(context.Background(), filesystem.NewRawFile("file.txt", "bar\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "different content of the file \"file.txt\"")
}

func TestAssertDirectoryDifferentContentWildcards(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	expected := "%c%c%c%c\n" // 4 chars
	require.NoError(t, expectedFs.WriteFile(context.Background(), filesystem.NewRawFile("file.txt", expected)))

	// File in actual - different content
	actual := "foo\n" // 3 chars
	require.NoError(t, actualFs.WriteFile(context.Background(), filesystem.NewRawFile("file.txt", actual)))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "different content of the file \"file.txt\"")
}

func TestAssertDirectorySameEmpty(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectoryIgnoreHiddenFiles(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	hiddenFilePath := filesystem.Join("myDir", ".hidden")
	require.NoError(t, expectedFs.WriteFile(context.Background(), filesystem.NewRawFile(hiddenFilePath, "foo\n")))

	// File in actual
	require.NoError(t, actualFs.WriteFile(context.Background(), filesystem.NewRawFile(hiddenFilePath, "bar\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectorySame(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	filePath := filesystem.Join("myDir", "file.txt")
	require.NoError(t, expectedFs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, "foo\n")))

	// File in actual
	require.NoError(t, actualFs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, "foo\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectorySameWildcards(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	filePath := filesystem.Join("myDir", "file.txt")
	require.NoError(t, expectedFs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, "%c%c%c\n")))

	// File in actual
	require.NoError(t, actualFs.WriteFile(context.Background(), filesystem.NewRawFile(filePath, "foo\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

// newMockedT - mocked version of *testing.T.
func newMockedT() *mockedT {
	return &mockedT{buf: bytes.NewBuffer(nil)}
}

func TestNormalize(t *testing.T) {
	t.Parallel()

	type args struct {
		input string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test with .json file",
			args: args{
				input: "/main/other/keboola.orchestrator/flow-mgmt-jira-instance/phases/003-load/001-keboola-wr-snowflake-10960305/task.json",
			},
			want: "/main/other/keboola.orchestrator/flow-mgmt-jira-instance/phases/003-load/001-keboola-wr-snowflake-%s/task.json",
		},
		{
			name: "Test with .yaml file (numeric suffix at the end)",
			args: args{
				input: "/main/other/keboola.orchestrator/flow-mgmt-jira-instance/phases/003-load/001-keboola-wr-snowflake-98765432/task.yaml",
			},
			want: "/main/other/keboola.orchestrator/flow-mgmt-jira-instance/phases/003-load/001-keboola-wr-snowflake-%s/task.yaml",
		},
		{
			name: "Test with path without digits before file extension",
			args: args{
				input: "/main/other/keboola.orchestrator/flow-mgmt-jira-instance/phases/003-load/001-keboola-wr-snowflake",
			},
			want: "/main/other/keboola.orchestrator/flow-mgmt-jira-instance/phases/003-load/001-keboola-wr-snowflake",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, Normalize(t, tt.args.input), "Normalize(%v, %v)", t, tt.args.input)
		})
	}
}
