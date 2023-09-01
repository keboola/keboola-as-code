package count

import (
	"bytes"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCounter(t *testing.T) {
	t.Parallel()

	c := NewCounter()

	// Empty
	assert.Equal(t, uint64(0), c.Count())

	// Empty
	assert.Equal(t, uint64(0), c.Count())

	// Add 0
	c.Add(0)
	assert.Equal(t, uint64(0), c.Count())

	// Add 3
	c.Add(3)
	assert.Equal(t, uint64(3), c.Count())

	// Add 2
	c.Add(2)
	assert.Equal(t, uint64(5), c.Count())
}

func TestMeterWithBackup(t *testing.T) {
	t.Parallel()

	backupPath := filepath.Join(t.TempDir(), "backup")
	m, err := NewCounterWithBackupFile(backupPath)
	assert.NoError(t, err)

	// Empty
	assert.Equal(t, uint64(0), m.Count())

	// Add 0
	m.Add(0)
	assert.Equal(t, uint64(0), m.Count())

	// Add 3
	m.Add(3)
	assert.Equal(t, uint64(3), m.Count())

	// Add 2
	m.Add(2)
	assert.Equal(t, uint64(5), m.Count())

	// Flush backup
	assert.NoError(t, m.Flush())
	content, err := os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, "5", string(content))

	// Add 4
	m.Add(4)
	assert.Equal(t, uint64(9), m.Count())

	// Close (flush backup)
	assert.NoError(t, m.Close())
	content, err = os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, "9", string(content))

	// Reopen - load from backup
	m, err = NewCounterWithBackupFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), m.Count())

	// Add 6
	m.Add(6)
	assert.Equal(t, uint64(15), m.Count())

	// Close
	assert.NoError(t, m.Close())
	content, err = os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, "15", string(content))
}

func TestMeterWithBackup_OpenError_Missing(t *testing.T) {
	t.Parallel()

	// Read error
	_, err := NewCounterWithBackupFile("/missing/file")
	assert.Error(t, err)
}

func TestMeterWithBackup_OpenError_Invalid(t *testing.T) {
	t.Parallel()

	backupPath := filepath.Join(t.TempDir(), "backup")
	assert.NoError(t, os.WriteFile(backupPath, []byte("foo"), 0o640))

	_, err := NewCounterWithBackupFile(backupPath)
	if assert.Error(t, err) {
		assert.Equal(t, `content "foo" of the backup file is not valid uint64`, err.Error())
	}
}

func TestMeterWithBackup_ReadError(t *testing.T) {
	t.Parallel()

	// Read error
	backupBuf := &testBuffer{}
	backupBuf.readError = errors.New("some read error")
	_, err := NewCounterWithBackup(backupBuf)
	if assert.Error(t, err) {
		assert.Equal(t, "cannot read from the backup file: some read error", err.Error())
	}
}

func TestMeterWithBackup_FlushError(t *testing.T) {
	t.Parallel()

	backupBuf := &testBuffer{}
	m, err := NewCounterWithBackup(backupBuf)
	assert.NoError(t, err)

	// Seek error
	backupBuf.seekError = errors.New("some seek error")
	err = m.Flush()
	if assert.Error(t, err) {
		assert.Equal(t, "cannot seek the backup file: some seek error", err.Error())
	}

	// Write error
	backupBuf.seekError = nil
	backupBuf.writeError = errors.New("some write error")
	err = m.Flush()
	if assert.Error(t, err) {
		assert.Equal(t, "cannot write to the backup file: some write error", err.Error())
	}
}

func TestMeterWithBackup_CloseError(t *testing.T) {
	t.Parallel()

	backupBuf := &testBuffer{}
	m, err := NewCounterWithBackup(backupBuf)
	assert.NoError(t, err)

	// Write error
	backupBuf.writeError = errors.New("some write error")
	err = m.Close()
	if assert.Error(t, err) {
		assert.Equal(t, "cannot write to the backup file: some write error", err.Error())
	}

	// Close error
	backupBuf.writeError = nil
	backupBuf.closeError = errors.New("some close error")
	err = m.Close()
	if assert.Error(t, err) {
		assert.Equal(t, "cannot close the backup file: some close error", err.Error())
	}
}

type testBuffer struct {
	bytes.Buffer
	readError  error
	seekError  error
	writeError error
	closeError error
}

func (w *testBuffer) Seek(offset int64, whence int) (ret int64, err error) {
	if w.seekError != nil {
		return 0, w.seekError
	}
	if offset == 0 && whence == io.SeekStart {
		w.Buffer.Reset()
	} else {
		panic(errors.New("unexpected seek"))
	}
	return 0, nil
}

func (w *testBuffer) Read(p []byte) (n int, err error) {
	if w.readError != nil {
		return 0, w.readError
	}
	return w.Buffer.Read(p)
}

func (w *testBuffer) Write(p []byte) (int, error) {
	if w.writeError != nil {
		return 0, w.writeError
	}
	return w.Buffer.Write(p)
}

func (w *testBuffer) Close() error {
	return w.closeError
}
