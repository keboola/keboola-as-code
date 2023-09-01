// Package count provides a counter with backup to a file.
package count

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"go.uber.org/atomic"
	"io"
	"os"
	"strconv"
	"strings"
)

// Counter is an atomic counter.
type Counter struct {
	value *atomic.Uint64
}

// CounterWithBackup in addition to Counter allows to read and save the counter value to a backup file.
type CounterWithBackup struct {
	*Counter
	backup backup
}

// backup interface contains used methods from *os.File.
type backup interface {
	Read(p []byte) (n int, err error)
	Write(p []byte) (n int, err error)
	Seek(offset int64, whence int) (ret int64, err error)
	Close() error
}

func NewCounter() *Counter {
	return &Counter{value: atomic.NewUint64(0)}
}

func NewCounterWithBackupFile(filePath string) (*CounterWithBackup, error) {
	backupFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}

	return NewCounterWithBackup(backupFile)
}

func NewCounterWithBackup(backup backup) (*CounterWithBackup, error) {
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
	c := &CounterWithBackup{Counter: NewCounter(), backup: backup}
	c.value.Store(value)
	return c, nil
}

func (c *Counter) Add(n uint64) uint64 {
	return c.value.Add(n)
}

func (c *Counter) Count() uint64 {
	return c.value.Load()
}

func (c *CounterWithBackup) Flush() error {
	// Seek to the beginning of the file
	// The size counter can only grow, so it guarantees that the entire file will be overwritten.
	if _, err := c.backup.Seek(0, io.SeekStart); err != nil {
		return errors.Errorf(`cannot seek the backup file: %w`, err)
	}

	if _, err := c.backup.Write([]byte(strconv.FormatUint(c.value.Load(), 10))); err != nil {
		return errors.Errorf(`cannot write to the backup file: %w`, err)
	}

	return nil
}

func (c *CounterWithBackup) Close() error {
	if err := c.Flush(); err != nil {
		return err
	}

	if err := c.backup.Close(); err != nil {
		return errors.Errorf(`cannot close the backup file: %w`, err)
	}

	return nil
}
