package fastcsv

import (
	"bytes"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/csvfmt"
)

// writer is simplified and optimized version of CSV writer.
type writer struct {
	out io.Writer
	row bytes.Buffer
}

func newWriter(out io.Writer) *writer {
	return &writer{out: out}
}

func (w *writer) WriteRow(cols *[]any) error {
	// Reset re-used buffer
	w.row.Reset()

	// Write each column
	for n, col := range *cols {
		// Cast the value to string
		toWrite, err := csvfmt.Format(col)
		if err != nil {
			return ValueError{ColumnIndex: n, err: err}
		}

		// Comma between values
		if n > 0 {
			if _, err := w.row.WriteRune(','); err != nil {
				return err
			}
		}

		// Value start quote
		if _, err := w.row.WriteRune('"'); err != nil {
			return err
		}

		// Write all until a special character
		for len(toWrite) > 0 {
			// Search for special characters
			stop := bytes.IndexRune(toWrite, '"')
			if stop < 0 {
				stop = len(toWrite)
			}

			// Copy verbatim everything before the special character.
			if _, err := w.row.Write(toWrite[:stop]); err != nil {
				return err
			}

			toWrite = toWrite[stop:]

			// Encode the special character
			if len(toWrite) > 0 && toWrite[0] == '"' {
				if _, err := w.row.WriteString(`""`); err != nil {
					return err
				}
				toWrite = toWrite[1:]
			}
		}

		// Value end quote
		if _, err = w.row.WriteRune('"'); err != nil {
			return err
		}
	}

	// Row separator
	_, err := w.row.WriteRune('\n')
	if err != nil {
		return err
	}

	// Flush the whole row or nothing
	_, err = w.row.WriteTo(w.out)
	if err != nil {
		return err
	}

	return nil
}
