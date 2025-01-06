// Package count provides a counter with backup to a file.
package count

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Counter is an atomic counter.
type Counter struct {
	lock    *sync.RWMutex
	count   uint64
	firstAt utctime.UTCTime
	lastAt  utctime.UTCTime
}

// CounterWithBackup in addition to Counter allows to read and save the counter value to a backup file.
// The backup is saved automatically if backupInterval > 0 , and manually using the SyncBackup or the Close methods.
type CounterWithBackup struct {
	*Counter
	backup       backup
	backupTicker clockwork.Ticker
}

// backup interface contains used methods from *os.File.
type backup interface {
	Read(p []byte) (n int, err error)
	Write(p []byte) (n int, err error)
	Seek(offset int64, whence int) (ret int64, err error)
	Close() error
}

func NewCounter() *Counter {
	return &Counter{lock: &sync.RWMutex{}}
}

func NewCounterWithBackupFile(ctx context.Context, clk clockwork.Clock, logger log.Logger, backupPath string, backupInterval time.Duration) (*CounterWithBackup, error) {
	backupFile, err := os.OpenFile(backupPath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}

	counter, err := NewCounterWithBackup(ctx, clk, logger, backupFile, backupInterval)
	if err != nil {
		_ = backupFile.Close()
		return nil, err
	}

	return counter, nil
}

func NewCounterWithBackup(ctx context.Context, clk clockwork.Clock, logger log.Logger, backup backup, backupInterval time.Duration) (*CounterWithBackup, error) {
	c := &CounterWithBackup{Counter: NewCounter(), backup: backup}

	// Read value
	buffer := make([]byte, 128)
	n, err := io.ReadFull(backup, buffer)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, errors.Errorf(`cannot read from the backup file: %w`, err)
	}

	// Parse values
	content := string(buffer[0:n])
	if content != "" {
		errs := errors.NewMultiError()
		parts := strings.Split(content, ",")
		if len(parts) != 3 {
			errs.Append(errors.Errorf(`expected 3 comma-separated values, found %d`, len(parts)))
		} else {
			if c.count, err = strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64); err != nil {
				errs.Append(errors.Errorf(`invalid count "%s"`, parts[0]))
			}
			if c.firstAt, err = utctime.Parse(parts[1]); err != nil {
				errs.Append(errors.Errorf(`invalid firstAt time "%s"`, parts[1]))
			}
			if c.lastAt, err = utctime.Parse(parts[2]); err != nil {
				errs.Append(errors.Errorf(`invalid lastAt time "%s"`, parts[2]))
			}
		}

		if err := errs.ErrorOrNil(); err != nil {
			content = strhelper.Truncate(content, 20, "...")
			return nil, errors.Errorf(`content "%s" of the backup file is not valid: %w`, content, err)
		}
	}

	// Start backup ticker
	if backupInterval > 0 {
		c.backupTicker = clk.NewTicker(backupInterval)
		go func() {
			for range c.backupTicker.Chan() {
				if err = c.SyncBackup(); err != nil {
					err = errors.PrefixErrorf(err, `cannot flush counter backup %v`, backup)
					logger.Error(ctx, err.Error())
				}
			}
		}()
	}

	return c, nil
}

func (c *Counter) Add(timestamp time.Time, n uint64) uint64 {
	if n == 0 {
		return c.Count()
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.count += n

	timestampUTC := utctime.From(timestamp)

	if c.firstAt.IsZero() || c.firstAt.After(timestampUTC) {
		c.firstAt = timestampUTC
	}

	if timestampUTC.After(c.lastAt) {
		c.lastAt = timestampUTC
	}

	return c.count
}

func (c *Counter) Count() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.count
}

func (c *Counter) FirstAt() utctime.UTCTime {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.firstAt
}

func (c *Counter) LastAt() utctime.UTCTime {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.lastAt
}

func (c *CounterWithBackup) SyncBackup() error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// Seek to the beginning of the file
	// The size counter can only grow, so it guarantees that the entire file will be overwritten.
	if _, err := c.backup.Seek(0, io.SeekStart); err != nil {
		return errors.Errorf(`cannot seek the backup file: %w`, err)
	}

	// Prepare file content
	var content bytes.Buffer
	content.WriteString(strconv.FormatUint(c.count, 10))
	content.WriteString(",")
	content.WriteString(c.firstAt.String())
	content.WriteString(",")
	content.WriteString(c.lastAt.String())

	// Write file content
	if _, err := c.backup.Write(content.Bytes()); err != nil {
		return errors.Errorf(`cannot write to the backup file: %w`, err)
	}

	return nil
}

func (c *CounterWithBackup) Close() error {
	if c.backupTicker != nil {
		c.backupTicker.Stop()
	}

	if err := c.SyncBackup(); err != nil {
		return err
	}

	if err := c.backup.Close(); err != nil {
		return errors.Errorf(`cannot close the backup file: %w`, err)
	}

	return nil
}
