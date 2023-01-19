package upload

import (
	"bytes"
	"context"
	"io"
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (u *Uploader) newRecordsReader(ctx context.Context, slice model.Slice) io.Reader {
	out, in := io.Pipe()
	go func() {
		var err error
		defer func() {
			if closeErr := in.CloseWithError(err); closeErr != nil {
				u.logger.Errorf(`cannot close records reader pipe: %w`, closeErr)
			}
		}()

		count := uint64(0)
		id := slice.IDRange.Start
		idPlaceholder := []byte(column.IDPlaceholder)
		if id < 1 {
			panic(errors.Errorf(`record ID must be > 0, found "%v"`, id))
		}

		// Read records
		records := u.schema.Records().InSlice(slice.SliceKey).GetAll().Do(ctx, u.etcdClient)
		for records.Next() {
			row := records.Value().Value
			row = bytes.ReplaceAll(row, idPlaceholder, []byte(strconv.FormatUint(id, 10)))
			_, err = in.Write(row)
			if err != nil {
				return
			}
			count++
			id++
		}

		// Check iterator error
		err = records.Err()
		if err != nil {
			return
		}

		// Check records count
		if count != slice.Statistics.RecordsCount {
			u.logger.Errorf(
				`unexpected number of uploaded records, expected "%d", found "%d"`,
				slice.Statistics.RecordsCount, count,
			)
		}
	}()
	return out
}
