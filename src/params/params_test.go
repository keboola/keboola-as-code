package params

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkingDirFromOs(t *testing.T) {
	logger, _, _ := utils.NewDebugLogger()
	flags := &Flags{}
	params, err := NewParams(logger, flags)

	// Assert
	assert.NoError(t, err)
	wd, err := os.Getwd()
	assert.NoError(t, err)
	assert.Equal(t, wd, params.WorkingDirectory)
}

func TestWorkingDirFromFlag(t *testing.T) {
	logger, _, _ := utils.NewDebugLogger()
	flags := &Flags{WorkingDirectory: "/test/abc"}
	params, err := NewParams(logger, flags)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "/test/abc", params.WorkingDirectory)
}

func TestProjectDirNotFound(t *testing.T) {
	logger, writer, _ := utils.NewDebugLogger()
	flags := &Flags{}
	params, err := NewParams(logger, flags)
	assert.NoError(t, err)
	err = writer.Flush()
	assert.NoError(t, err)

	// Assert
	assert.Empty(t, params.ProjectDirectory)
}

func TestProjectDirExpectedDirButFoundFile(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	fakeMetadataFile := filepath.Join(projectDir, "foo", ".keboola")
	workingDir := filepath.Join(projectDir, "foo", "bar", "baz")

	// Create ".keboola" dir in project directory
	assert.NoError(t, os.Mkdir(metadataDir, 0600))

	// Working dir = project sub-dir
	assert.NoError(t, os.MkdirAll(workingDir, 0600))
	assert.NoError(t, os.Chdir(workingDir))

	// Create ".keboola" file in "foo" dir -> invalid ".keboola" should be dir
	file, err := os.Create(fakeMetadataFile)
	assert.NoError(t, err)
	assert.NoError(t, file.Close())

	logger, writer, buffer := utils.NewDebugLogger()
	flags := &Flags{}
	params, err := NewParams(logger, flags)
	assert.NoError(t, err)
	err = writer.Flush()
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, projectDir, params.ProjectDirectory)
	assert.Equal(t, fmt.Sprintf("DEBUG  Expected dir, but found file at \"%s\"\n", fakeMetadataFile), buffer.String())
}

func TestProjectDirSameAsWorkingDir(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")

	// Create ".keboola" dir in project directory
	assert.NoError(t, os.Mkdir(metadataDir, 0600))

	// Working dir = project dir
	assert.NoError(t, os.Chdir(projectDir))

	logger, writer, buffer := utils.NewDebugLogger()
	flags := &Flags{}
	params, err := NewParams(logger, flags)
	assert.NoError(t, err)
	err = writer.Flush()
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, projectDir, params.ProjectDirectory)
	assert.Empty(t, buffer.String())
}

func TestProjectDirIsParentOfWorkingDir(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	workingDir := filepath.Join(projectDir, "foo", "bar", "baz")

	// Create ".keboola" dir in project directory
	assert.NoError(t, os.Mkdir(metadataDir, 0600))

	// Working dir = project dir sub-dir
	assert.NoError(t, os.MkdirAll(workingDir, 0600))
	assert.NoError(t, os.Chdir(workingDir))

	logger, writer, buffer := utils.NewDebugLogger()
	flags := &Flags{}
	params, err := NewParams(logger, flags)
	assert.NoError(t, err)
	err = writer.Flush()
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, projectDir, params.ProjectDirectory)
	assert.Empty(t, buffer.String())
}

func TestValidateNoRequired(t *testing.T) {
	params := &Params{}
	assert.Empty(t, params.Validate(Required{}))
}

func TestValidateAllRequired(t *testing.T) {
	params := &Params{}
	errors := params.Validate(Required{
		ProjectDirectory: true,
		ApiUrl:           true,
		ApiToken:         true,
	})

	// Assert
	expected := []string{
		`- This or any parent directory is not a Keboola project dir.`,
		`  Project directory must contain ".keboola" metadata directory.`,
		`  Please change working directory to a project directory or create a new with "init" command.`,
		`- Missing API URL. Please use "--api-url" flag or ENV variable "KBC_STORAGE_API_URL".`,
		`- Missing API token. Please use "--token" flag or ENV variable "KBC_STORAGE_API_TOKEN".`,
	}
	assert.Equal(t, strings.Join(expected, "\n"), errors)
}
