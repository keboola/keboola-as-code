package service

import (
	"bytes"
	"context"
	"io"
	"strconv"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func newRecordsReader(ctx context.Context, logger log.Logger, client *etcd.Client, schema *schema.Schema, slice model.Slice, receivedStats statistics.Value, uploadStats *statistics.AfterUpload, pageSize int) io.Reader {
	out, in := io.Pipe()
	go func() {
		var err error
		defer func() {
			if closeErr := in.CloseWithError(err); closeErr != nil {
				logger.Errorf(`cannot close records reader pipe: %s`, closeErr)
			}
		}()

		id := slice.IDRange.Start
		idPlaceholder := []byte(column.IDPlaceholder)
		if id < 1 {
			panic(errors.Errorf(`record ID must be > 0, found "%v"`, id))
		}

		// Read records.
		// It is guaranteed that new records are not added to the prefix, and existing ones are not changed.
		// Therefore, the WithFromSameRev(false) is used.
		// This also prevents ErrCompacted, because we are not requesting a specific version.
		records := schema.Records().InSlice(slice.SliceKey)
		itr := records.GetAll(iterator.WithPageSize(pageSize), iterator.WithFromSameRev(false)).Do(ctx, client)
		for itr.Next() {
			row := itr.Value().Value
			row = bytes.ReplaceAll(row, idPlaceholder, []byte(strconv.FormatUint(id, 10)))
			_, err = in.Write(row)
			if err != nil {
				return
			}
			uploadStats.RecordsCount++
			id++
		}

		// Check iterator error
		err = itr.Err()
		if err != nil {
			return
		}

		// Check records count
		if uploadStats.RecordsCount != receivedStats.RecordsCount {
			logger.Errorf(
				`unexpected number of uploaded records, expected "%d", found "%d"`,
				receivedStats.RecordsCount, uploadStats.RecordsCount,
			)
		}
	}()
	return out
}
