package count

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestCounter(t *testing.T) {
	t.Parallel()

	c := NewCounter()

	// Empty
	assert.Equal(t, uint64(0), c.Count())

	// Add 0
	c.Add(utctime.MustParse("2000-01-01T00:00:00.000Z").Time(), 0)
	assert.Equal(t, uint64(0), c.Count())
	assert.True(t, c.FirstAt().IsZero())
	assert.True(t, c.LastAt().IsZero())

	// Add 3
	c.Add(utctime.MustParse("2001-01-01T00:00:00.000Z").Time(), 3)
	assert.Equal(t, uint64(3), c.Count())
	assert.Equal(t, utctime.MustParse("2001-01-01T00:00:00.000Z"), c.FirstAt())
	assert.Equal(t, utctime.MustParse("2001-01-01T00:00:00.000Z"), c.LastAt())

	// Add 2
	c.Add(utctime.MustParse("2002-01-01T00:00:00.000Z").Time(), 2)
	assert.Equal(t, uint64(5), c.Count())
	assert.Equal(t, utctime.MustParse("2001-01-01T00:00:00.000Z"), c.FirstAt())
	assert.Equal(t, utctime.MustParse("2002-01-01T00:00:00.000Z"), c.LastAt())
}

func TestMeterWithBackup(t *testing.T) {
	t.Parallel()

	backupPath := filepath.Join(t.TempDir(), "backup")
	c, err := NewCounterWithBackupFile(backupPath)
	assert.NoError(t, err)

	// Empty
	assert.Equal(t, uint64(0), c.Count())

	// Add 0
	c.Add(utctime.MustParse("2000-01-01T00:00:00.000Z").Time(), 0)
	assert.Equal(t, uint64(0), c.Count())
	assert.True(t, c.FirstAt().IsZero())
	assert.True(t, c.LastAt().IsZero())

	// Add 3
	c.Add(utctime.MustParse("2001-01-01T00:00:00.000Z").Time(), 3)
	assert.Equal(t, uint64(3), c.Count())
	assert.Equal(t, utctime.MustParse("2001-01-01T00:00:00.000Z"), c.FirstAt())
	assert.Equal(t, utctime.MustParse("2001-01-01T00:00:00.000Z"), c.LastAt())

	// Add 2
	c.Add(utctime.MustParse("2002-01-01T00:00:00.000Z").Time(), 2)
	assert.Equal(t, uint64(5), c.Count())
	assert.Equal(t, utctime.MustParse("2001-01-01T00:00:00.000Z"), c.FirstAt())
	assert.Equal(t, utctime.MustParse("2002-01-01T00:00:00.000Z"), c.LastAt())

	// Flush backup
	assert.NoError(t, c.Flush())
	content, err := os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, "5,2001-01-01T00:00:00.000Z,2002-01-01T00:00:00.000Z", string(content))

	// Add 4
	c.Add(utctime.MustParse("2003-01-01T00:00:00.000Z").Time(), 4)
	assert.Equal(t, uint64(9), c.Count())
	assert.Equal(t, utctime.MustParse("2001-01-01T00:00:00.000Z"), c.FirstAt())
	assert.Equal(t, utctime.MustParse("2003-01-01T00:00:00.000Z"), c.LastAt())

	// Close (flush backup)
	assert.NoError(t, c.Close())
	content, err = os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, "9,2001-01-01T00:00:00.000Z,2003-01-01T00:00:00.000Z", string(content))

	// Reopen - load from backup
	c, err = NewCounterWithBackupFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), c.Count())

	// Add 6
	c.Add(utctime.MustParse("2004-01-01T00:00:00.000Z").Time(), 6)
	assert.Equal(t, uint64(15), c.Count())
	assert.Equal(t, utctime.MustParse("2001-01-01T00:00:00.000Z"), c.FirstAt())
	assert.Equal(t, utctime.MustParse("2004-01-01T00:00:00.000Z"), c.LastAt())

	// Close
	assert.NoError(t, c.Close())
	content, err = os.ReadFile(backupPath)
	assert.NoError(t, err)
	assert.Equal(t, "15,2001-01-01T00:00:00.000Z,2004-01-01T00:00:00.000Z", string(content))
}

func TestMeterWithBackup_OpenError_Missing(t *testing.T) {
	t.Parallel()

	// Read error
	_, err := NewCounterWithBackupFile("/missing/file")
	assert.Error(t, err)
}

func TestMeterWithBackup_OpenError_Invalid(t *testing.T) {
	t.Parallel()

	cases := []struct{ name, content, expectedError string }{
		{
			"commas",
			"foo",
			`content "%s" of the backup file is not valid: expected 3 comma-separated values, found 1`,
		},
		{
			"count",
			"foo,2001-01-01T00:00:00.000Z,2004-01-01T00:00:00.000Z",
			`content "%s" of the backup file is not valid: invalid count "foo"`,
		},
		{
			"firstAt",
			"123,foo,2004-01-01T00:00:00.000Z",
			`content "%s" of the backup file is not valid: invalid firstAt time "foo"`,
		},
		{
			"lastAt",
			"123,2001-01-01T00:00:00.000Z,foo",
			`content "%s" of the backup file is not valid: invalid lastAt time "foo"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backupPath := filepath.Join(t.TempDir(), "backup")
			assert.NoError(t, os.WriteFile(backupPath, []byte(tc.content), 0o640))

			_, err := NewCounterWithBackupFile(backupPath)
			if assert.Error(t, err) {
				wildcards.Assert(t, tc.expectedError, err.Error())
			}
		})
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
