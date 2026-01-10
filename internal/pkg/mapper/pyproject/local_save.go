package pyproject

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// MapBeforeLocalSave generates pyproject.toml for Python transformations.
func (m *pyprojectMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	// Only for Python transformation configs
	if !isPythonTransformation(recipe.Object) {
		return nil
	}

	config := recipe.Object.(*model.Config)

	// Generate pyproject.toml
	content := generatePyProjectToml(config)

	// Write the file
	pyprojectPath := m.state.NamingGenerator().PyProjectFilePath(recipe.Path())
	recipe.Files.
		Add(filesystem.NewRawFile(pyprojectPath, content)).
		SetDescription("Python dependencies").
		AddTag(model.FileTypeOther)

	return nil
}

// isPythonTransformation checks if the object is a Python transformation config.
func isPythonTransformation(object model.Object) bool {
	config, ok := object.(*model.Config)
	if !ok {
		return false
	}

	componentID := config.ComponentID.String()
	return strings.Contains(componentID, "python") &&
		(strings.Contains(componentID, "transformation") ||
			componentID == "keboola.python-mlflow" ||
			componentID == "kds-team.app-custom-python")
}

// generatePyProjectToml generates the pyproject.toml content.
func generatePyProjectToml(config *model.Config) string {
	var sb strings.Builder

	// Project name (slugified config name)
	projectName := strhelper.NormalizeName(config.Name)
	if projectName == "" {
		projectName = "transformation"
	}

	// Get packages from config
	packages := getPackagesFromConfig(config)

	// Write [project] section
	sb.WriteString("[project]\n")
	sb.WriteString(fmt.Sprintf("name = \"%s\"\n", projectName))
	sb.WriteString("version = \"1.0.0\"\n")
	sb.WriteString("requires-python = \">=3.11\"\n")
	sb.WriteString("\n")

	// Write dependencies
	sb.WriteString("dependencies = [\n")
	for _, pkg := range packages {
		sb.WriteString(fmt.Sprintf("    \"%s\",\n", pkg))
	}
	sb.WriteString("]\n")
	sb.WriteString("\n")

	// Write [project.optional-dependencies] section
	sb.WriteString("[project.optional-dependencies]\n")
	sb.WriteString("dev = [\n")
	sb.WriteString("    \"pytest>=7.0.0\",\n")
	sb.WriteString("    \"mypy>=1.0.0\",\n")
	sb.WriteString("]\n")
	sb.WriteString("\n")

	// Write [tool.keboola] section
	sb.WriteString("[tool.keboola]\n")
	sb.WriteString(fmt.Sprintf("component_id = \"%s\"\n", config.ComponentID))
	sb.WriteString(fmt.Sprintf("config_id = \"%s\"\n", config.ID))

	return sb.String()
}

// getPackagesFromConfig extracts packages from config.Content["parameters"]["packages"].
func getPackagesFromConfig(config *model.Config) []string {
	var packages []string

	if config.Content == nil {
		return packages
	}

	// Get parameters
	parametersRaw, ok := config.Content.Get("parameters")
	if !ok {
		return packages
	}

	parameters, ok := parametersRaw.(map[string]any)
	if !ok {
		return packages
	}

	// Get packages
	packagesRaw, ok := parameters["packages"]
	if !ok {
		return packages
	}

	// Handle different package formats
	switch pkgs := packagesRaw.(type) {
	case []any:
		for _, pkg := range pkgs {
			if pkgStr, ok := pkg.(string); ok {
				packages = append(packages, pkgStr)
			}
		}
	case []string:
		packages = append(packages, pkgs...)
	}

	return packages
}

// FileKindPyProject is the file kind tag for pyproject.toml files.
const FileKindPyProject = naming.PyProjectFile
