package preview

import (
	"encoding/csv"
	"fmt"
	"math"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"golang.org/x/term"

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
	b := &strings.Builder{}
	w := csv.NewWriter(b)
	_ = w.Write(table.Columns)
	_ = w.WriteAll(table.Rows)
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
	// try to calculate max width of each column using terminal size
	maxWidth := math.MaxInt
	if term.IsTerminal(0) {
		maxWidth, _, _ = term.GetSize(0)
		// account for borders+padding
		maxWidth -= 1 + len(table.Columns)*3
	}
	widths := measureColumns(table, maxWidth)

	var b strings.Builder

	truncate := func(s string, max int) string {
		if len(s) < 3 {
			return s
		}
		if len(s) > max-3 {
			return fmt.Sprintf("%s...", s[:max-3])
		}
		return s
	}
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
			fmt.Fprintf(&b, " %-*s ", w, truncate(data[i], w-2))
			b.WriteString(boxV)
		}
		fmt.Fprintf(&b, " %-*s ", last, truncate(data[len(data)-1], last-2))
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

func measureColumns(table *keboola.TablePreview, maxWidth int) []int {
	// each column requests its width based on the maximum width of its content
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

	// then we attempt to fit all of that content on the user's screen
	// by truncating content as necessary to fit `maxWidth`
	totalWidth := 0
	for _, width := range widths {
		totalWidth += width
	}
	if totalWidth > maxWidth {
		remainingWidth := maxWidth
		for i, width := range widths {
			maxColumnWidth := remainingWidth / (len(table.Columns) - i)
			if width <= maxColumnWidth {
				remainingWidth -= width
			} else {
				remainingWidth -= maxColumnWidth
				widths[i] = maxColumnWidth
			}
		}
	}

	return widths
}
