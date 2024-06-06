package size

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestMeter(t *testing.T) {
	t.Parallel()

	out := &testBuffer{}
	m := NewMeter(out)

	// Empty
	assert.Equal(t, datasize.ByteSize(0), m.Size())

	// No data
	n, err := m.Write([]byte{})
	assert.Equal(t, 0, n)
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(0), m.Size())

	// Data
	n, err = m.Write([]byte("foo"))
	assert.Equal(t, datasize.ByteSize(3), m.Size())
	assert.Equal(t, 3, n)
	require.NoError(t, err)

	// Data
	n, err = m.Write([]byte("bar"))
	assert.Equal(t, datasize.ByteSize(6), m.Size())
	assert.Equal(t, 3, n)
	require.NoError(t, err)

	// Error
	out.writeError = errors.New("some error")
	n, err = m.Write([]byte("baz"))
	assert.Equal(t, datasize.ByteSize(6), m.Size())
	assert.Equal(t, 0, n)
	if assert.Error(t, err) {
		assert.Equal(t, "some error", err.Error())
	}
}

func TestMeterWithBackup(t *testing.T) {
	t.Parallel()

	out := &testBuffer{}
	backupPath := filepath.Join(t.TempDir(), "backup")
	m, err := NewMeterWithBackupFile(out, backupPath)
	require.NoError(t, err)

	// Empty
	assert.Equal(t, datasize.ByteSize(0), m.Size())

	// No data
	n, err := m.Write([]byte{})
	assert.Equal(t, 0, n)
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(0), m.Size())

	// Data
	n, err = m.Write([]byte("foo"))
	assert.Equal(t, datasize.ByteSize(3), m.Size())
	assert.Equal(t, 3, n)
	require.NoError(t, err)

	// Data
	n, err = m.Write([]byte("bar"))
	assert.Equal(t, datasize.ByteSize(6), m.Size())
	assert.Equal(t, 3, n)
	require.NoError(t, err)

	// Error
	out.writeError = errors.New("some error")
	n, err = m.Write([]byte("baz"))
	assert.Equal(t, datasize.ByteSize(6), m.Size())
	assert.Equal(t, 0, n)
	if assert.Error(t, err) {
		assert.Equal(t, "some error", err.Error())
	}

	// Flush backup
	require.NoError(t, m.Flush())
	content, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, "6", string(content))

	// Data
	out.writeError = nil
	n, err = m.Write([]byte("baz"))
	assert.Equal(t, datasize.ByteSize(9), m.Size())
	assert.Equal(t, 3, n)
	require.NoError(t, err)

	// Close (flush backup)
	require.NoError(t, m.Close())
	content, err = os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, "9", string(content))

	// Reopen - load from backup
	m, err = NewMeterWithBackupFile(out, backupPath)
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(9), m.Size())

	// Data
	out.writeError = nil
	n, err = m.Write([]byte("123456"))
	assert.Equal(t, datasize.ByteSize(15), m.Size())
	assert.Equal(t, 6, n)
	require.NoError(t, err)

	// Close
	require.NoError(t, m.Close())
	content, err = os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, "15", string(content))
}

func TestMeterWithBackup_OpenError_Missing(t *testing.T) {
	t.Parallel()

	// Read error
	_, err := NewMeterWithBackupFile(&testBuffer{}, "/missing/file")
	assert.Error(t, err)
}

func TestMeterWithBackup_OpenError_Invalid(t *testing.T) {
	t.Parallel()

	backupPath := filepath.Join(t.TempDir(), "backup")
	require.NoError(t, os.WriteFile(backupPath, []byte("foo"), 0o640))

	_, err := NewMeterWithBackupFile(&testBuffer{}, backupPath)
	if assert.Error(t, err) {
		assert.Equal(t, `content "foo" of the backup file is not valid uint64`, err.Error())
	}
}

func TestMeterWithBackup_ReadError(t *testing.T) {
	t.Parallel()

	// Read error
	backupBuf := &testBuffer{}
	backupBuf.readError = errors.New("some read error")
	_, err := NewMeterWithBackup(&testBuffer{}, backupBuf)
	if assert.Error(t, err) {
		assert.Equal(t, "cannot read from the backup file: some read error", err.Error())
	}
}

func TestMeterWithBackup_FlushError(t *testing.T) {
	t.Parallel()

	backupBuf := &testBuffer{}
	m, err := NewMeterWithBackup(&testBuffer{}, backupBuf)
	require.NoError(t, err)

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
	m, err := NewMeterWithBackup(&testBuffer{}, backupBuf)
	require.NoError(t, err)

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
