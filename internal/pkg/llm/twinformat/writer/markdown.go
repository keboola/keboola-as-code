package writer

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MarkdownWriter writes Markdown files to the filesystem.
type MarkdownWriter struct {
	fs filesystem.Fs
}

// NewMarkdownWriter creates a new Markdown writer.
func NewMarkdownWriter(fs filesystem.Fs) *MarkdownWriter {
	return &MarkdownWriter{fs: fs}
}

// Write writes a Markdown file to the specified path.
func (w *MarkdownWriter) Write(ctx context.Context, path string, content string) error {
	// Ensure trailing newline.
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}

	if err := w.fs.WriteFile(ctx, filesystem.NewRawFile(path, content)); err != nil {
		return errors.Errorf("failed to write Markdown file %s: %w", path, err)
	}

	return nil
}

// MarkdownBuilder helps build Markdown content.
type MarkdownBuilder struct {
	lines []string
}

// ProjectStats holds statistics for README generation.
type ProjectStats struct {
	TotalBuckets         int
	TotalTables          int
	TotalTransformations int
	TotalEdges           int
}

// NewMarkdownBuilder creates a new Markdown builder.
func NewMarkdownBuilder() *MarkdownBuilder {
	return &MarkdownBuilder{
		lines: make([]string, 0),
	}
}

// H1 adds a level 1 heading.
func (b *MarkdownBuilder) H1(text string) *MarkdownBuilder {
	b.lines = append(b.lines, "# "+text, "")
	return b
}

// H2 adds a level 2 heading.
func (b *MarkdownBuilder) H2(text string) *MarkdownBuilder {
	b.lines = append(b.lines, "## "+text, "")
	return b
}

// H3 adds a level 3 heading.
func (b *MarkdownBuilder) H3(text string) *MarkdownBuilder {
	b.lines = append(b.lines, "### "+text, "")
	return b
}

// P adds a paragraph.
func (b *MarkdownBuilder) P(text string) *MarkdownBuilder {
	b.lines = append(b.lines, text, "")
	return b
}

// List adds a bulleted list.
func (b *MarkdownBuilder) List(items []string) *MarkdownBuilder {
	for _, item := range items {
		b.lines = append(b.lines, "- "+item)
	}
	b.lines = append(b.lines, "")
	return b
}

// Code adds a code block.
func (b *MarkdownBuilder) Code(language, code string) *MarkdownBuilder {
	b.lines = append(b.lines, "```"+language)
	b.lines = append(b.lines, code)
	b.lines = append(b.lines, "```", "")
	return b
}

// Table adds a Markdown table.
func (b *MarkdownBuilder) Table(headers []string, rows [][]string) *MarkdownBuilder {
	if len(headers) == 0 {
		return b
	}

	// Header row.
	b.lines = append(b.lines, "| "+strings.Join(headers, " | ")+" |")

	// Separator row.
	separators := make([]string, len(headers))
	for i := range separators {
		separators[i] = "---"
	}
	b.lines = append(b.lines, "| "+strings.Join(separators, " | ")+" |")

	// Data rows.
	for _, row := range rows {
		// Pad row to match header length.
		paddedRow := make([]string, len(headers))
		for i := range paddedRow {
			if i < len(row) {
				paddedRow[i] = row[i]
			} else {
				paddedRow[i] = ""
			}
		}
		b.lines = append(b.lines, "| "+strings.Join(paddedRow, " | ")+" |")
	}
	b.lines = append(b.lines, "")
	return b
}

// Line adds a raw line.
func (b *MarkdownBuilder) Line(text string) *MarkdownBuilder {
	b.lines = append(b.lines, text)
	return b
}

// Blank adds a blank line.
func (b *MarkdownBuilder) Blank() *MarkdownBuilder {
	b.lines = append(b.lines, "")
	return b
}

// HR adds a horizontal rule.
func (b *MarkdownBuilder) HR() *MarkdownBuilder {
	b.lines = append(b.lines, "---", "")
	return b
}

// String returns the built Markdown content.
func (b *MarkdownBuilder) String() string {
	return strings.Join(b.lines, "\n")
}

// GenerateProjectREADME generates the main README.md for the twin format output.
func GenerateProjectREADME(projectID string, stats ProjectStats) string {
	mb := NewMarkdownBuilder()

	mb.H1(fmt.Sprintf("Keboola Project: %s", projectID))
	mb.P("This directory contains an AI-optimized export of a Keboola project in \"twin format\".")

	mb.H2("Quick Start")
	mb.P("For AI assistants: Start with `manifest-extended.json` for a complete project overview.")

	mb.H2("Statistics")
	mb.List([]string{
		fmt.Sprintf("Buckets: %d", stats.TotalBuckets),
		fmt.Sprintf("Tables: %d", stats.TotalTables),
		fmt.Sprintf("Transformations: %d", stats.TotalTransformations),
		fmt.Sprintf("Lineage Edges: %d", stats.TotalEdges),
	})

	mb.H2("Directory Structure")
	mb.Code("", `./
├── manifest.yaml              # Project configuration
├── manifest-extended.json     # Complete project overview (start here)
├── README.md                  # This file
├── buckets/                   # Storage buckets and tables
├── transformations/           # Data transformations
├── components/                # Extractors, writers, etc.
├── jobs/                      # Job execution history
├── indices/                   # Lineage graph and queries
├── ai/                        # AI assistant guides
└── samples/                   # Table data samples (when --with-samples)`)

	mb.H2("Key Files")
	mb.Table(
		[]string{"File", "Purpose"},
		[][]string{
			{"manifest-extended.json", "Complete project overview in one file"},
			{"buckets/index.json", "Catalog of all buckets"},
			{"transformations/index.json", "Catalog of all transformations"},
			{"indices/graph.jsonl", "Lineage graph (JSONL format)"},
			{"samples/index.json", "Index of table samples (when --with-samples)"},
		},
	)

	mb.H2("Format Version")
	mb.P("Twin Format Version: 1")
	mb.P("Format Version: 2")

	return mb.String()
}
