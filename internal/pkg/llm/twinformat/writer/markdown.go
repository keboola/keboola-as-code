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
	mb.Code("", `twin_format/
├── manifest.yaml              # Project configuration
├── manifest-extended.json     # Complete project overview (start here)
├── README.md                  # This file
├── buckets/                   # Storage buckets and tables
├── transformations/           # Data transformations
├── components/                # Extractors, writers, etc.
├── jobs/                      # Job execution history
└── indices/                   # Lineage graph and queries`)

	mb.H2("Key Files")
	mb.Table(
		[]string{"File", "Purpose"},
		[][]string{
			{"manifest-extended.json", "Complete project overview in one file"},
			{"buckets/index.json", "Catalog of all buckets"},
			{"transformations/index.json", "Catalog of all transformations"},
			{"indices/graph.jsonl", "Lineage graph (JSONL format)"},
		},
	)

	mb.H2("Format Version")
	mb.P("Twin Format Version: 1")
	mb.P("Format Version: 2")

	return mb.String()
}

// GenerateAIGuide generates the ai/README.md guide for AI assistants.
func GenerateAIGuide(projectID string, stats ProjectStats, platforms map[string]int, sources []string) string {
	mb := NewMarkdownBuilder()

	mb.H1("AI Assistant Guide")
	mb.P(fmt.Sprintf("This guide helps AI assistants understand and work with Keboola project `%s`.", projectID))

	mb.H2("Project Overview")
	mb.List([]string{
		fmt.Sprintf("Total Buckets: %d", stats.TotalBuckets),
		fmt.Sprintf("Total Tables: %d", stats.TotalTables),
		fmt.Sprintf("Total Transformations: %d", stats.TotalTransformations),
		fmt.Sprintf("Total Lineage Edges: %d", stats.TotalEdges),
	})

	mb.H2("Data Sources")
	if len(sources) > 0 {
		mb.List(sources)
	} else {
		mb.P("No data sources detected.")
	}

	mb.H2("Transformation Platforms")
	if len(platforms) > 0 {
		platformList := make([]string, 0, len(platforms))
		for platform, count := range platforms {
			platformList = append(platformList, fmt.Sprintf("%s: %d transformations", platform, count))
		}
		mb.List(platformList)
	} else {
		mb.P("No transformations found.")
	}

	mb.H2("How to Use This Data")
	mb.H3("Understanding Data Flow")
	mb.P("Use `indices/graph.jsonl` to understand how data flows through the project:")
	mb.List([]string{
		"`consumed_by` edges: Table → Transformation (input)",
		"`produces` edges: Transformation → Table (output)",
	})

	mb.H3("Finding Tables")
	mb.P("Tables are organized by bucket in `buckets/{bucket}/tables/{table}/metadata.json`.")

	mb.H3("Finding Transformations")
	mb.P("Transformations are in `transformations/{name}/metadata.json` with their dependencies and job status.")

	mb.H3("Checking Job Status")
	mb.P("Recent job executions are in `jobs/recent/` and `jobs/by-component/`.")

	mb.H2("JSON Documentation Fields")
	mb.P("Every JSON file includes documentation fields:")
	mb.List([]string{
		"`_comment`: How the data was generated",
		"`_purpose`: Why this file exists",
		"`_update_frequency`: When to regenerate",
	})

	return mb.String()
}

// ProjectStats holds statistics for README generation.
type ProjectStats struct {
	TotalBuckets         int
	TotalTables          int
	TotalTransformations int
	TotalEdges           int
}
