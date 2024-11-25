package primarykey

import (
	"bytes"
	"go/format"
	"os"
	"path/filepath"

	"entgo.io/ent/entc"
	"entgo.io/ent/entc/gen"
	"entgo.io/ent/entc/load"
	"golang.org/x/tools/imports"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const keyPgkName = "key"

// GenerateKeys generates a key struct for each schema with the primary key Mixin.
func GenerateKeys(config *gen.Config) error {
	// Load schema definitions
	graph, err := entc.LoadGraph(config.Schema, config)
	if err != nil {
		return errors.Errorf("cannot load schema: %w", err)
	}

	// Create target dir
	targetDir := filepath.Join(graph.Target, keyPgkName)
	if err := os.Mkdir(targetDir, 0o744); err != nil {
		return err
	}

	// Create Go file with a key struct for each schema
	for _, schema := range graph.Schemas {
		for _, field := range schema.Fields {
			if field.Name == "id" {
				if asMap, ok := field.Annotations[pkAnnotationName]; ok {
					// asMap is PKAnnotation type serialized to a map
					pkAnnotation := PKAnnotation{}
					if err := json.ConvertByJSON(asMap, &pkAnnotation); err != nil {
						return err
					}
					if err := generateKey(targetDir, schema, pkAnnotation); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func generateKey(targetDir string, schema *load.Schema, pkAnnotation PKAnnotation) error {
	// Get all imports
	importsMap := make(map[string]struct{})
	for _, field := range pkAnnotation.Fields {
		importsMap[field.GoType.PkgPath] = struct{}{}
	}
	importPkg := make([]string, 0, len(importsMap))
	for pkgPath := range importsMap {
		importPkg = append(importPkg, pkgPath)
	}

	// Data is in the template accessible by $ variable
	data := map[string]any{
		"Imports":   importPkg,
		"Fields":    pkAnnotation.Fields,
		"KeyStruct": keyStructName(schema.Name),
	}

	// Load template
	tmpl, err := loadTemplate("key.tmpl", nil)
	if err != nil {
		return err
	}

	// Execute the template
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, data)
	if err != nil {
		return errors.Errorf(`cannot execute template "%s": %w`, tmpl.Name(), err)
	}

	// Format code
	filePath := filepath.Join(targetDir, strhelper.FirstLower(schema.Name)+"Key.go")
	code, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}
	code, err = imports.Process(filePath, code, nil)
	if err != nil {
		return err
	}

	// Write file
	return os.WriteFile(filePath, code, 0o644)
}

func keyStructName(schemaName string) string {
	return schemaName + "Key"
}
