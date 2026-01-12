package pyproject

import (
	"bufio"
	"context"
	"regexp"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterLocalLoad parses pyproject.toml and updates config packages.
func (m *pyprojectMapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	// Only for Python transformation configs
	if !isPythonTransformation(recipe.Object) {
		return nil
	}

	config := recipe.Object.(*model.Config)
	pyprojectPath := m.state.NamingGenerator().PyProjectFilePath(recipe.ObjectManifest.Path())

	// Check if pyproject.toml exists
	if !m.state.ObjectsRoot().IsFile(ctx, pyprojectPath) {
		return nil
	}

	// Read the file
	file, err := recipe.Files.
		Load(pyprojectPath).
		AddMetadata(filesystem.ObjectKeyMetadata, config.Key()).
		SetDescription("Python dependencies").
		AddTag(model.FileTypeOther).
		ReadFile(ctx)
	if err != nil {
		return err
	}

	// Parse packages from pyproject.toml
	packages := parsePyProjectPackages(file.Content)

	// Update config.Content with packages
	if len(packages) > 0 {
		updateConfigPackages(config, packages)
	}

	return nil
}

// parsePyProjectPackages extracts dependencies from pyproject.toml content.
func parsePyProjectPackages(content string) []string {
	var packages []string
	inDependencies := false

	// Simple parser for TOML dependencies section
	// Looking for:
	// dependencies = [
	//     "package>=version",
	// ]
	scanner := bufio.NewScanner(strings.NewReader(content))
	depPattern := regexp.MustCompile(`^\s*"([^"]+)"`)

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "dependencies = [") || strings.HasPrefix(trimmed, "dependencies=[") {
			inDependencies = true
			continue
		}

		if inDependencies {
			if trimmed == "]" {
				inDependencies = false
				continue
			}

			// Extract package from quoted string
			if match := depPattern.FindStringSubmatch(line); len(match) > 1 {
				packages = append(packages, match[1])
			}
		}
	}

	return packages
}

// updateConfigPackages updates the config.Content with the packages list.
func updateConfigPackages(config *model.Config, packages []string) {
	if config.Content == nil {
		return
	}

	// Convert packages to []any for storage
	packagesAny := make([]any, len(packages))
	for i, pkg := range packages {
		packagesAny[i] = pkg
	}

	// Get or create parameters
	parametersRaw, ok := config.Content.Get("parameters")
	if !ok {
		// Create new parameters orderedmap if it doesn't exist
		params := orderedmap.New()
		params.Set("packages", packagesAny)
		config.Content.Set("parameters", params)
		return
	}

	// Handle both orderedmap and map[string]any types
	switch params := parametersRaw.(type) {
	case *orderedmap.OrderedMap:
		params.Set("packages", packagesAny)
	case map[string]any:
		params["packages"] = packagesAny
	}
}
