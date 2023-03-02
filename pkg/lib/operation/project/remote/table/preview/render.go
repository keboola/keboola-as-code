package preview

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	TableFormatPretty = "pretty"
	TableFormatCSV    = "csv"
	TableFormatJSON   = "json"
)

func IsValidFormat(format string) bool {
	switch format {
	case TableFormatJSON, TableFormatCSV, TableFormatPretty:
		return true
	default:
		return false
	}
}

func renderTable(table *keboola.TablePreview, format string) (string, error) {
	switch format {
	case TableFormatJSON:
		return renderJSON(table), nil
	case TableFormatCSV:
		return renderCSV(table), nil
	case TableFormatPretty:
		return renderPretty(table), nil
	default:
		return "", errors.Errorf("invalid table format %s", format)
	}
}

func renderJSON(table *keboola.TablePreview) string {
	type output struct {
		Columns []string   `json:"columns"`
		Rows    [][]string `json:"rows"`
	}

	o := output{Columns: table.Columns, Rows: table.Rows}

	return json.MustEncodeString(o, false)
}

func renderCSV(table *keboola.TablePreview) string {
	var b strings.Builder

	for _, col := range table.Columns[:len(table.Columns)-1] {
		b.WriteString(col)
		b.WriteString(",")
	}
	b.WriteString(table.Columns[len(table.Columns)-1])
	b.WriteString("\n")

	for _, row := range table.Rows[:len(table.Rows)-1] {
		for _, col := range row[:len(row)-1] {
			b.WriteString(col)
			b.WriteString(",")
		}
		b.WriteString(row[len(row)-1])
		b.WriteString("\n")
	}
	lastRow := table.Rows[len(table.Rows)-1]
	for _, col := range lastRow[:len(lastRow)-1] {
		b.WriteString(col)
		b.WriteString(",")
	}
	b.WriteString(lastRow[len(lastRow)-1])

	return b.String()
}

const (
	boxV  = "┃"
	boxH  = "━"
	boxTL = "┏"
	boxTR = "┓"
	boxBL = "┗"
	boxBR = "┛"
	boxVL = "┣"
	boxVR = "┫"
	boxHT = "┳"
	boxHB = "┻"
	boxC  = "╋"
)

func renderPretty(table *keboola.TablePreview) string {
	widths := measureColumns(table)

	var b strings.Builder

	// draws a "border" line, e.g. `┏━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━┓`
	border := func(left, middle, right string, lf bool) {
		b.WriteString(left)
		cols, last := widths[:len(widths)-1], widths[len(widths)-1]
		for _, w := range cols {
			b.WriteString(strings.Repeat(boxH, w+2))
			b.WriteString(middle)
		}
		b.WriteString(strings.Repeat(boxH, last+2))
		b.WriteString(right)
		if lf {
			b.WriteString("\n")
		}
	}
	// draws a "content" line, e.g. `┃ asdf    ┃ my data ┃ 2015-09-01 ┃`
	content := func(data []string) {
		b.WriteString(boxV)
		cols, last := widths[:len(widths)-1], widths[len(widths)-1]
		for i, w := range cols {
			fmt.Fprintf(&b, " %-*s ", w, data[i])
			b.WriteString(boxV)
		}
		fmt.Fprintf(&b, " %-*s ", last, data[len(data)-1])
		b.WriteString(boxV)
		b.WriteString("\n")
	}

	border(boxTL, boxHT, boxTR, true)
	content(table.Columns)
	border(boxVL, boxC, boxVR, true)
	for _, row := range table.Rows {
		content(row)
	}
	border(boxBL, boxHB, boxBR, false)

	return b.String()
}

func measureColumns(table *keboola.TablePreview) []int {
	widths := make([]int, len(table.Columns))
	for i, col := range table.Columns {
		widths[i] = len(col)
	}
	for _, row := range table.Rows {
		for i, col := range row {
			if len(col) > widths[i] {
				widths[i] = len(col)
			}
		}
	}
	return widths
}
