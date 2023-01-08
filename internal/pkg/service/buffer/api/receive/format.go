package receive

import (
	"encoding/csv"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func FormatCSVRow(ctx *receivectx.Context, mapping model.Mapping) (string, error) {
	errs := errors.NewMultiErrorNoTrace()
	csvRow := make([]string, len(mapping.Columns))

	// Get value for each column
	i := 0
	for _, column := range mapping.Columns {
		if csvCol, err := column.CSVValue(ctx); err == nil {
			csvRow[i] = csvCol
		} else {
			errs.Append(err)
		}
		i++
	}

	// Request body is lazy parsed, when first used in a Jsonnet template.
	// Jsonnet library strips error type and HTTP status code,
	// so the error is checked from the context and has priority.
	if err := ctx.ParseBodyErr(); err != nil {
		return "", err
	}

	if errs.Len() > 0 {
		return "", errs.ErrorOrNil()
	}

	// Generate CSV row
	var str strings.Builder
	wr := csv.NewWriter(&str)
	if err := wr.Write(csvRow); err != nil {
		return "", err
	}
	wr.Flush()
	if err := wr.Error(); err != nil {
		return "", err
	}

	return str.String(), nil
}
