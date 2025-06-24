package

// Package size allows to measure how much data has passed through a writer.
size

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jonboulle/clockwork"
	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Meter implements io.Writer interface to measure size of the data written to the underlying writer.
type Meter struct {
	w    io.Writer
	lock deadlock.Mutex
	size datasize.ByteSize
}

// MeterWithBackup in addition to Meter allows to read and save the counter value to a backup file.
// The backup is saved automatically if backupInterval > 0 , and manually using the SyncBackup or the Close methods.
type MeterWithBackup struct {
	*Meter
	backup       backup
	backupTicker clockwork.Ticker
	lock         deadlock.RWMutex
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

func NewMeterWithBackupFile(ctx context.Context, clk clockwork.Clock, logger log.Logger, w io.Writer, backupPath string, backupInterval time.Duration) (*MeterWithBackup, error) {
	backupFile, err := os.OpenFile(backupPath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}

	meter, err := NewMeterWithBackup(ctx, clk, logger, w, backupFile, backupInterval)
	if err != nil {
		_ = backupFile.Close()
		return nil, err
	}

	return meter, nil
}

func NewMeterWithBackup(ctx context.Context, clk clockwork.Clock, logger log.Logger, w io.Writer, backup backup, backupInterval time.Duration) (*MeterWithBackup, error) {
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
	m := &MeterWithBackup{Meter: NewMeter(w), backup: backup}
	m.size = datasize.ByteSize(value)

	// Start backup ticker
	if backupInterval > 0 {
		m.backupTicker = clk.NewTicker(backupInterval)
		go func() {
			for range m.backupTicker.Chan() {
				if err = m.SyncBackup(); err != nil {
					err = errors.PrefixErrorf(err, `cannot flush meter backup %v`, backup)
					logger.Error(ctx, err.Error())
				}
			}
		}()
	}

	return m, nil
}

func (m *Meter) Write(p []byte) (int, error) {
	n, err := m.w.Write(p)
	m.lock.Lock()
	defer m.lock.Unlock()
	m.size += datasize.ByteSize(n) //nolint:gosec // write returns always non negative numbers
	return n, err
}

func (m *Meter) Size() datasize.ByteSize {
	m.lock.Lock()
	defer m.lock.Unlock()
	size := m.size
	return size
}

func (w *MeterWithBackup) SyncBackup() error {
	w.lock.RLock()
	defer w.lock.RUnlock()

	return w.syncBackup()
}

func (w *MeterWithBackup) syncBackup() error {
	if w.backup == nil {
		return nil
	}

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
	if w.backupTicker != nil {
		w.backupTicker.Stop()
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.syncBackup(); err != nil {
		return err
	}

	if err := w.backup.Close(); err != nil {
		return errors.Errorf(`cannot close the backup file: %w`, err)
	}

	w.backup = nil

	return nil
}
