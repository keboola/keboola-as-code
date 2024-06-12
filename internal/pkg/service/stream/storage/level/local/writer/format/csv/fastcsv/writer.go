package fastcsv

import (
	"bytes"
	"io"
	"strconv"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
		toWrite, err := w.toCSVValue(col)
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
			stop := strings.IndexRune(toWrite, '"')
			if stop < 0 {
				stop = len(toWrite)
			}

			// Copy verbatim everything before the special character.
			if _, err := w.row.WriteString(toWrite[:stop]); err != nil {
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

func (w *writer) toCSVValue(v any) (string, error) {
	// Inspired by cast.ToStringE(), but without slow reflection
	switch s := v.(type) {
	case string:
		return s, nil
	case bool:
		return strconv.FormatBool(s), nil
	case float64:
		return strconv.FormatFloat(s, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(s), 'f', -1, 32), nil
	case int:
		return strconv.Itoa(s), nil
	case int64:
		return strconv.FormatInt(s, 10), nil
	case int32:
		return strconv.Itoa(int(s)), nil
	case int16:
		return strconv.FormatInt(int64(s), 10), nil
	case int8:
		return strconv.FormatInt(int64(s), 10), nil
	case uint:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint64:
		return strconv.FormatUint(s, 10), nil
	case uint32:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(s), 10), nil
	case []byte:
		return string(s), nil
	case nil:
		return "", nil
	default:
		return "", errors.Errorf("unable to cast %#v of type %T to string", v, v)
	}
}
