package writer

import (
	"context"
	"encoding/csv"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CSVWriter writes CSV files to the filesystem.
type CSVWriter struct {
	fs filesystem.Fs
}

// NewCSVWriter creates a new CSV writer.
func NewCSVWriter(fs filesystem.Fs) *CSVWriter {
	return &CSVWriter{fs: fs}
}

// Write writes a CSV file to the specified path.
// The first row is treated as headers, and subsequent rows are data.
func (w *CSVWriter) Write(ctx context.Context, path string, headers []string, rows [][]string) error {
	var sb strings.Builder
	csvWriter := csv.NewWriter(&sb)

	// Write headers.
	if err := csvWriter.Write(headers); err != nil {
		return errors.Errorf("failed to write CSV headers: %w", err)
	}

	// Write data rows, normalizing length to match headers.
	for i, row := range rows {
		normalizedRow := make([]string, len(headers))
		copy(normalizedRow, row)

		if err := csvWriter.Write(normalizedRow); err != nil {
			return errors.Errorf("failed to write CSV row %d: %w", i, err)
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return errors.Errorf("failed to flush CSV writer: %w", err)
	}

	content := sb.String()
	if err := w.fs.WriteFile(ctx, filesystem.NewRawFile(path, content)); err != nil {
		return errors.Errorf("failed to write CSV file %s: %w", path, err)
	}

	return nil
}

// WriteFromData writes a CSV file from a slice of maps.
// Each map represents a row, with keys as column names.
func (w *CSVWriter) WriteFromData(ctx context.Context, path string, columns []string, data []map[string]string) error {
	rows := make([][]string, 0, len(data))

	for _, row := range data {
		rowData := make([]string, len(columns))
		for i, col := range columns {
			rowData[i] = row[col]
		}
		rows = append(rows, rowData)
	}

	return w.Write(ctx, path, columns, rows)
}

// WriteRaw writes raw CSV content to the specified path.
// This is useful when the CSV data is already formatted.
func (w *CSVWriter) WriteRaw(ctx context.Context, path string, content string) error {
	if err := w.fs.WriteFile(ctx, filesystem.NewRawFile(path, content)); err != nil {
		return errors.Errorf("failed to write CSV file %s: %w", path, err)
	}
	return nil
}
