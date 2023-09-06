package csv

import (
	"bufio"
	"encoding/csv"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	compressionWriter "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/base"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/count"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/size"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/spf13/cast"
	"io"
	"path/filepath"
	"sync"
)

const (
	RowsCounterFile      = "rows_count"
	CompressedSizeFile   = "compressed_size"
	UncompressedSizeFile = "uncompressed_size"
	fileBufferSize       = 64 * datasize.KB
)

type Writer struct {
	base    *base.Writer
	columns column.Columns
	writeWg *sync.WaitGroup
	// csvWriter writers to the Chain in the baseWriter.
	csvWriter *csv.Writer
	// csvWriterLock serializes writes to the internal csv.Writer buffer
	csvWriterLock *sync.Mutex
	// rowsCounter counts successfully written rows.
	rowsCounter *count.CounterWithBackup
	// compressedMeter measures size of the data after compression, if any.
	compressedMeter *size.MeterWithBackup
	// uncompressedMeter measures size of the data before compression, if any.
	uncompressedMeter *size.MeterWithBackup
}

func NewWriter(b *base.Writer) (w *Writer, err error) {
	w = &Writer{
		base:          b,
		columns:       b.Columns(),
		writeWg:       &sync.WaitGroup{},
		csvWriterLock: &sync.Mutex{},
	}

	// Measure size of compressed data
	_, err = w.base.PrependWriterOrErr(func(writer writechain.Writer) (out io.Writer, err error) {
		w.compressedMeter, err = size.NewMeterWithBackupFile(writer, filepath.Join(w.base.DirPath(), CompressedSizeFile))
		return w.compressedMeter, err
	})
	if err != nil {
		return nil, err
	}

	// Add compression if enabled
	if compConfig := w.base.Compression(); compConfig.Type != compression.TypeNone {
		// Add compression writer
		_, err := w.base.PrependWriterOrErr(func(writer writechain.Writer) (io.Writer, error) {
			return compressionWriter.New(writer, compConfig)
		})
		if err != nil {
			return nil, err
		}

		// Measure size of uncompressed CSV data
		_, err = w.base.PrependWriterOrErr(func(writer writechain.Writer) (_ io.Writer, err error) {
			w.uncompressedMeter, err = size.NewMeterWithBackupFile(writer, filepath.Join(w.base.DirPath(), UncompressedSizeFile))
			return w.uncompressedMeter, err
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Size of the compressed and uncompressed data is same
		w.uncompressedMeter = w.compressedMeter

		// Add a small buffer before the file
		w.base.PrependWriter(func(writer writechain.Writer) io.Writer {
			return bufio.NewWriterSize(writer, int(fileBufferSize.Bytes()))
		})
	}

	// Setup rows counter
	w.rowsCounter, err = count.NewCounterWithBackupFile(filepath.Join(w.base.DirPath(), RowsCounterFile))
	if err == nil {
		// Backup the counter value on Flush and Close
		w.base.PrependFlusherCloser(w.rowsCounter)
	} else {
		return nil, err
	}

	// Setup CSV writer
	w.csvWriter = csv.NewWriter(w.base)
	w.base.PrependFlushFn(w.csvWriter, func() error {
		w.csvWriterLock.Lock()
		w.csvWriter.Flush()
		err = w.csvWriter.Error()
		w.csvWriterLock.Unlock()
		return err
	})

	return w, nil
}

func (w *Writer) WriteRow(values []any) error {
	// Block Close method
	w.writeWg.Add(1)
	defer w.writeWg.Done()

	// Check values count
	if len(values) != len(w.columns) {
		return errors.Errorf(`expected %d columns in the row, given %d`, len(w.columns), len(values))
	}

	// Cast all values to string
	var err error
	strings := make([]string, len(w.columns))
	for i, v := range values {
		if strings[i], err = cast.ToStringE(v); err != nil {
			columnName := w.columns[i].ColumnName()
			return errors.Errorf(`cannot convert value of the column "%s" to the string: %w`, columnName, err)
		}
	}

	// Write CSV row
	// This WriteRow method can be called multiple times in parallel.
	// One write to the CSV Writer invokes multiple writes to the underlying writers,
	// so a lock must be used to prevent data mix-up.
	notifier, err := w.base.DoWithNotify(func() error {
		w.csvWriterLock.Lock()
		err = w.csvWriter.Write(strings)
		w.csvWriterLock.Unlock()
		w.base.AddWriteOp(1)
		return err
	})

	// Return writer error
	if err != nil {
		return err
	}

	// Wait for sync to disk, return sync error, if any
	err = notifier.Wait()
	if err != nil {
		return err
	}

	w.rowsCounter.Add(1)
	return nil
}

func (w *Writer) DumpChain() string {
	return w.base.Dump()
}


// WaitingWriteOps returns count of write operations waiting for the sync, for tests.
func (w *Writer) WaitingWriteOps() uint64 {
	return w.base.WaitingWriteOps()
}
func (w *Writer) RowsCount() uint64 {
	return w.rowsCounter.Count()
}

func (w *Writer) CompressedSize() datasize.ByteSize {
	return w.compressedMeter.Size()
}

func (w *Writer) UncompressedSize() datasize.ByteSize {
	return w.uncompressedMeter.Size()
}

func (w *Writer) SliceKey() storage.SliceKey {
	return w.base.SliceKey()
}

func (w *Writer) DirPath() string {
	return w.base.DirPath()
}

func (w *Writer) FilePath() string {
	return w.base.FilePath()
}

func (w *Writer) Close() error {

	// Close the chain
	err := w.base.Close()

	// Wait for running writes
	w.writeWg.Wait()

	return err
}
