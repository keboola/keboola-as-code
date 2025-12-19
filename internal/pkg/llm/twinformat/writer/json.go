package writer

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// JSONWriter writes JSON files to the filesystem.
type JSONWriter struct {
	fs filesystem.Fs
}

// NewJSONWriter creates a new JSON writer.
func NewJSONWriter(fs filesystem.Fs) *JSONWriter {
	return &JSONWriter{fs: fs}
}

// Write writes a JSON file to the specified path.
func (w *JSONWriter) Write(ctx context.Context, path string, data any) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return errors.Errorf("failed to marshal JSON: %w", err)
	}

	// Add trailing newline for consistency.
	content = append(content, '\n')

	if err := w.fs.WriteFile(ctx, filesystem.NewRawFile(path, string(content))); err != nil {
		return errors.Errorf("failed to write JSON file %s: %w", path, err)
	}

	return nil
}

// WriteWithDocFields writes a JSON file with documentation fields at the top.
// The docFields map is merged with the data map, with docFields taking precedence.
func (w *JSONWriter) WriteWithDocFields(ctx context.Context, path string, docFields map[string]string, data map[string]any) error {
	// Create a new map with doc fields first, then data.
	result := make(map[string]any)

	// Add doc fields first (they should appear at the top).
	for k, v := range docFields {
		result[k] = v
	}

	// Add data fields.
	maps.Copy(result, data)

	return w.Write(ctx, path, result)
}
