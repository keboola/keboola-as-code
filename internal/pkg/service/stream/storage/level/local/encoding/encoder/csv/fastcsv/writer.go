package fastcsv

import (
	"bytes"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/csvfmt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// writer is simplified and optimized version of CSV writer.
type writer struct {
	out   io.Writer
	row   bytes.Buffer
	limit uint64
}

func newWriter(out io.Writer, limit uint64) *writer {
	return &writer{out: out, limit: limit}
}

func (w *writer) WriteRow(cols *[]any) (int, error) {
	// Bytes written
	n := 0

	// Reset re-used buffer
	w.row.Reset()

	// Write each column
	for index, col := range *cols {
		// Cast the value to string
		toWrite, err := csvfmt.Format(col)
		if err != nil {
			return 0, ValueError{ColumnIndex: index, err: err}
		}

		// Comma between values
		if index > 0 {
			if b, err := w.row.WriteRune(','); err != nil {
				return 0, err
			} else {
				n += b
			}
		}

		// Value start quote
		if b, err := w.row.WriteRune('"'); err != nil {
			return 0, err
		} else {
			n += b
		}

		// Write all until a special character
		for len(toWrite) > 0 {
			// Search for special characters
			stop := bytes.IndexRune(toWrite, '"')
			if stop < 0 {
				stop = len(toWrite)
			}

			// Copy verbatim everything before the special character.
			if b, err := w.row.Write(toWrite[:stop]); err != nil {
				return 0, err
			} else {
				n += b
			}

			toWrite = toWrite[stop:]

			// Encode the special character
			if len(toWrite) > 0 && toWrite[0] == '"' {
				if b, err := w.row.WriteString(`""`); err != nil {
					return 0, err
				} else {
					n += b
				}
				toWrite = toWrite[1:]
			}
		}

		// Value end quote
		if b, err := w.row.WriteRune('"'); err != nil {
			return 0, err
		} else {
			n += b
		}

		// Check limit of single column
		if uint64(w.row.Len()) > w.limit {
			return 0, errors.New("too big csv row to be written")
		}
	}

	// Row separator
	if b, err := w.row.WriteRune('\n'); err != nil {
		return 0, err
	} else {
		n += b
	}

	// Flush the whole row or nothing
	if b, err := w.row.WriteTo(w.out); err != nil {
		return 0, err
	} else {
		n += int(b)
	}

	return n, nil
}
