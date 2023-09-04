package readchain

import (
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestChain_Empty tests that an empty Chain with only one reader.
func TestChain_Empty(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	// Read all from the Chain
	content, err := io.ReadAll(tc.Chain)
	if assert.NoError(tc.T, err) {
		assert.Equal(tc.T, "foo bar", string(content))
	}

	// Close the chain
	assert.NoError(t, tc.Chain.Close())
}

// TestChain_SetupMethods tests all setup methods.
func TestChain_SetupMethods(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	// Readers
	assert.True(t, tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return &testReader{inner: r, Name: "R1"}
	}))

	assert.False(t, tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return r
	}))

	assert.False(t, tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return nil
	}))

	ok, err := tc.Chain.PrependReaderOrErr(func(r io.Reader) (io.Reader, error) {
		return &testReadCloser{inner: r, Name: "R2"}, nil
	})
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = tc.Chain.PrependReaderOrErr(func(r io.Reader) (io.Reader, error) {
		return nil, errors.New("some error")
	})
	assert.False(t, ok)
	if assert.Error(t, err) {
		assert.Equal(t, "some error", err.Error())
	}

	ok, err = tc.Chain.PrependReaderOrErr(func(r io.Reader) (io.Reader, error) {
		return r, nil
	})
	assert.False(t, ok)
	assert.NoError(t, err)

	ok, err = tc.Chain.PrependReaderOrErr(func(r io.Reader) (io.Reader, error) {
		return nil, nil
	})
	assert.False(t, ok)
	assert.NoError(t, err)

	// Closers
	tc.Chain.PrependCloser(&testCloser{Name: "C1"})
	tc.Chain.PrependCloser(&testCloser{Name: "C2"})
	tc.Chain.PrependCloseFn("fn3", func() error { return nil })
	tc.Chain.AppendCloser(&testCloser{Name: "C3"})
	tc.Chain.AppendCloser(&testCloser{Name: "C4"})
	tc.Chain.AppendCloseFn("fn4", func() error { return nil })

	assert.Equal(t, `
Readers:
  R2
  R1
  file

Closers:
  fn3
  C2
  C1
  R2
  file
  C3
  C4
  fn4
`, "\n"+tc.Chain.Dump())
}

func TestChain_UnwrapFile_Ok(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "file")
	assert.NoError(t, os.WriteFile(path, []byte("foo bar"), 0o640))

	expectedFile, err := os.OpenFile(path, os.O_RDONLY, 0)
	assert.NoError(t, err)

	logger := log.NewDebugLogger()
	chain := New(logger, expectedFile)

	// Unwrap OK, it is the only element in the chain
	file, ok := chain.UnwrapFile()
	assert.Equal(t, expectedFile, file)
	assert.True(t, ok)

	// Unwrap not OK, it is not the only element in the chain
	chain.PrependReader(func(r io.Reader) io.Reader {
		return &testReader{inner: r}
	})
	file, ok = chain.UnwrapFile()
	assert.Nil(t, file)
	assert.False(t, ok)
}

func TestChain_UnwrapFile_NotOk(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	chain := New(logger, strings.NewReader("foo bar"))

	// Unwrap not OK, there is only one reader, but it is not *os.File
	file, ok := chain.UnwrapFile()
	assert.Nil(t, file)
	assert.False(t, ok)
}

// TestChain_ReadAndCloseOk tests successful Read and Close.
func TestChain_ReadAndCloseOk(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return &testReader{inner: r, Logger: tc.Logger, Name: "R1"}
	})
	tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return &testReadCloser{inner: r, Logger: tc.Logger, Name: "RC2"}
	})
	tc.Chain.PrependCloseFn("FN1", func() error {
		tc.Logger.Info(`TEST: close "FN1"`)
		return nil
	})
	tc.Chain.PrependCloseFn("FN2", func() error {
		tc.Logger.Info(`TEST: close "FN2"`)
		return nil
	})
	tc.Chain.AppendCloseFn("FN3", func() error {
		tc.Logger.Info(`TEST: close "FN3"`)
		return nil
	})
	tc.Chain.AppendCloseFn("FN4", func() error {
		tc.Logger.Info(`TEST: close "FN4"`)
		return nil
	})

	// Read all from the Chain
	content, err := io.ReadAll(tc.Chain)
	if assert.NoError(tc.T, err) {
		assert.Equal(tc.T, "foo bar", string(content))
	}

	// Close the chain
	assert.NoError(t, tc.Chain.Close())

	// 1st read is the content, 2nd is EOF error
	tc.AssertLogs(`
INFO  TEST: read "RC2"
INFO  TEST: read "R1"
INFO  TEST: read "file"
INFO  TEST: read "RC2"
INFO  TEST: read "R1"
INFO  TEST: read "file"
DEBUG  closing chain
INFO  TEST: close "FN2"
INFO  TEST: close "FN1"
INFO  TEST: close "RC2"
INFO  TEST: close "file"
INFO  TEST: close "FN3"
INFO  TEST: close "FN4"
DEBUG  chain closed
`)
}

// TestChain_ReadError tests a Read error.
func TestChain_ReadError(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return &testReader{inner: r, Logger: tc.Logger, Name: "R1", ReadError: errors.New("some error")}
	})
	tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return &testReadCloser{inner: r, Logger: tc.Logger, Name: "RC2"}
	})

	// Read all from the Chain
	_, err := io.ReadAll(tc.Chain)
	if assert.Error(tc.T, err) {
		assert.Equal(tc.T, "some error", err.Error())
	}

	tc.AssertLogs(`
INFO  TEST: read "RC2"
INFO  TEST: read "R1"
`)
}

// TestChain_CloseError tests a Close error.
func TestChain_CloseError(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return &testReadCloser{inner: r, Logger: tc.Logger, Name: "RC1"}
	})
	tc.Chain.PrependReader(func(r io.Reader) io.Reader {
		return &testReadCloser{inner: r, Logger: tc.Logger, Name: "RC2", CloseError: errors.New("some error")}
	})

	// Read all from the Chain
	content, err := io.ReadAll(tc.Chain)
	if assert.NoError(tc.T, err) {
		assert.Equal(tc.T, "foo bar", string(content))
	}

	// Read all from the Chain
	err = tc.Chain.Close()
	if assert.Error(tc.T, err) {
		assert.Equal(tc.T, "chain close error: cannot close \"RC2\": some error", err.Error())
	}

	// 1st read is the content, 2nd is EOF error
	tc.AssertLogs(`
INFO  TEST: read "RC2"
INFO  TEST: read "RC1"
INFO  TEST: read "file"
INFO  TEST: read "RC2"
INFO  TEST: read "RC1"
INFO  TEST: read "file"
DEBUG  closing chain
INFO  TEST: close "RC2"
ERROR  cannot close "RC2": some error
INFO  TEST: close "RC1"
INFO  TEST: close "file"
DEBUG  chain closed
`)
}

type chainTestCase struct {
	T      testing.TB
	Logger log.DebugLogger
	Chain  *Chain
}

func newChainTestCase(t testing.TB) *chainTestCase {
	t.Helper()
	logger := log.NewDebugLogger()
	testFile := &testReadCloser{inner: strings.NewReader("foo bar"), Logger: logger, Name: "file"}
	chain := New(logger, testFile)
	return &chainTestCase{T: t, Logger: logger, Chain: chain}
}

func (tc *chainTestCase) AssertLogs(expected string) bool {
	return wildcards.Assert(tc.T, strings.TrimSpace(expected), strings.TrimSpace(tc.Logger.AllMessages()))
}

type testReader struct {
	inner     io.Reader
	Name      string
	Logger    log.Logger
	ReadError error
}

func (r *testReader) String() string {
	return r.Name
}

func (r *testReader) Read(p []byte) (int, error) {
	r.Logger.Infof(`TEST: read "%s"`, r.Name)
	if r.ReadError != nil {
		return 0, r.ReadError
	}
	return r.inner.Read(p)
}

type testReadCloser struct {
	inner      io.Reader
	Name       string
	Logger     log.Logger
	ReadError  error
	CloseError error
}

func (r *testReadCloser) String() string {
	return r.Name
}

func (r *testReadCloser) Read(p []byte) (int, error) {
	r.Logger.Infof(`TEST: read "%s"`, r.Name)
	if r.ReadError != nil {
		return 0, r.ReadError
	}
	return r.inner.Read(p)
}

func (r *testReadCloser) Close() error {
	r.Logger.Infof(`TEST: close "%s"`, r.Name)
	return r.CloseError
}

type testCloser struct {
	Name       string
	Logger     log.Logger
	CloseError error
}

func (c *testCloser) String() string {
	return c.Name
}

func (c *testCloser) Close() error {
	c.Logger.Infof(`TEST: close "%s"`, c.Name)
	return c.CloseError
}
