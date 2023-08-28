// Package size allows to measure how much data has passed through a writer.
package size

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"io"
	"os"
	"strconv"
	"strings"
)

// Meter implements io.Writer interface to measure size of the data written to the underlying writer.
type Meter struct {
	w    io.Writer
	size datasize.ByteSize
}

// MeterWithBackup in addition to Meter allows to read and save the counter value to a backup file.
type MeterWithBackup struct {
	*Meter
	backup backup
}

// backup interface contains used methods from *os.File.
type backup interface {
	Read(p []byte) (n int, err error)
	Write(p []byte) (n int, err error)
	Seek(offset int64, whence int) (ret int64, err error)
	Close() error
}

func NewMeter(w io.Writer) *Meter {
	return &Meter{w: w}
}

func NewMeterWithBackupFile(w io.Writer, filePath string) (*MeterWithBackup, error) {
	backupFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}

	return NewMeterWithBackup(w, backupFile)
}

func NewMeterWithBackup(w io.Writer, backup backup) (*MeterWithBackup, error) {
	// Read value
	buffer := make([]byte, 32)
	n, err := io.ReadFull(backup, buffer)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, errors.Errorf(`cannot read from the backup file: %w`, err)
	}

	// Parse value
	var value uint64
	content := string(buffer[0:n])
	if content != "" {
		value, err = strconv.ParseUint(strings.TrimSpace(content), 10, 64)
		if err != nil {
			content = strhelper.Truncate(content, 20, "...")
			return nil, errors.Errorf(`content "%s" of the backup file is not valid uint64`, content)
		}
	}

	// Create writer and set the counter value
	out := &MeterWithBackup{Meter: NewMeter(w), backup: backup}
	out.size = datasize.ByteSize(value)
	return out, nil
}

func (w *Meter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.size += datasize.ByteSize(n)
	return n, err
}

func (w *Meter) Size() datasize.ByteSize {
	return w.size
}

func (w *MeterWithBackup) Flush() error {
	// Seek to the beginning of the file
	// The size counter can only grow, so it guarantees that the entire file will be overwritten.
	if _, err := w.backup.Seek(0, io.SeekStart); err != nil {
		return errors.Errorf(`cannot seek the backup file: %w`, err)
	}

	if _, err := w.backup.Write([]byte(strconv.FormatUint(uint64(w.size), 10))); err != nil {
		return errors.Errorf(`cannot write to the backup file: %w`, err)
	}

	return nil
}

func (w *MeterWithBackup) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}

	if err := w.backup.Close(); err != nil {
		return errors.Errorf(`cannot close the backup file: %w`, err)
	}

	return nil
}
