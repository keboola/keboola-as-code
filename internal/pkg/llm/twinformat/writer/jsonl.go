package writer

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// JSONLWriter writes JSONL (JSON Lines) files to the filesystem.
type JSONLWriter struct {
	fs filesystem.Fs
}

// NewJSONLWriter creates a new JSONL writer.
func NewJSONLWriter(fs filesystem.Fs) *JSONLWriter {
	return &JSONLWriter{fs: fs}
}

// Write writes a JSONL file to the specified path.
// Each item in the slice is written as a separate JSON line.
func (w *JSONLWriter) Write(ctx context.Context, path string, items []any) error {
	lines := make([]string, 0, len(items))

	for i, item := range items {
		line, err := json.Marshal(item)
		if err != nil {
			return errors.Errorf("failed to marshal JSONL item %d: %w", i, err)
		}
		lines = append(lines, string(line))
	}

	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}

	if err := w.fs.WriteFile(ctx, filesystem.NewRawFile(path, content)); err != nil {
		return errors.Errorf("failed to write JSONL file %s: %w", path, err)
	}

	return nil
}

// WriteWithMeta writes a JSONL file with a metadata line at the top.
// The meta object is written as the first line, followed by the items.
func (w *JSONLWriter) WriteWithMeta(ctx context.Context, path string, meta any, items []any) error {
	allItems := make([]any, 0, len(items)+1)
	allItems = append(allItems, meta)
	allItems = append(allItems, items...)
	return w.Write(ctx, path, allItems)
}
