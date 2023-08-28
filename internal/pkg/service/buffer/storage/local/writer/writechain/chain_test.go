package writechain

import (
	"bufio"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestChain_Empty tests that an empty Chain writes directly to the File.
func TestChain_Empty(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)
	tc.WriteData([]string{"foo", "bar"})
	tc.AssertFileContent("foobar")
}

// TestChain_SetupMethods tests all setup methods.
func TestChain_SetupMethods(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	// Writers
	assert.True(t, tc.Chain.PrependWriter(func(w Writer) io.Writer {
		return &testWriterSimple{Writer: w, Name: "W1"}
	}))

	assert.False(t, tc.Chain.PrependWriter(func(w Writer) io.Writer {
		return w
	}))

	assert.False(t, tc.Chain.PrependWriter(func(w Writer) io.Writer {
		return nil
	}))

	ok, err := tc.Chain.PrependWriterOrErr(func(w Writer) (io.Writer, error) {
		return &testWriterFlusherCloser{Writer: w, Name: "W2"}, nil
	})
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = tc.Chain.PrependWriterOrErr(func(w Writer) (io.Writer, error) {
		return w, nil
	})
	assert.False(t, ok)
	assert.NoError(t, err)

	ok, err = tc.Chain.PrependWriterOrErr(func(w Writer) (io.Writer, error) {
		return nil, nil
	})
	assert.False(t, ok)
	assert.NoError(t, err)

	// Flushers
	tc.Chain.PrependFlusher(&testFlusher{Name: "F1"})
	tc.Chain.PrependFlusher(&testFlusher{Name: "F2"})
	tc.Chain.PrependFlushFn(func() error { return nil })
	tc.Chain.AppendFlusher(&testFlusher{Name: "F3"})
	tc.Chain.AppendFlusher(&testFlusher{Name: "F4"})
	tc.Chain.AppendFlushFn(func() error { return nil })

	// Closers
	tc.Chain.PrependCloser(&testCloser{Name: "C1"})
	tc.Chain.PrependCloser(&testCloser{Name: "C2"})
	tc.Chain.PrependCloseFn(func() error { return nil })
	tc.Chain.AppendCloser(&testCloser{Name: "C3"})
	tc.Chain.AppendCloser(&testCloser{Name: "C4"})
	tc.Chain.AppendCloseFn(func() error { return nil })

	assert.Equal(t, `
Writers:
  W2 writer
  W1 writer

Flushers:
  writechain.flushFn
  F2 flusher
  F1 flusher
  W2 writer
  F3 flusher
  F4 flusher
  writechain.flushFn

Closers:
  writechain.closeFn
  C2 closer
  C1 closer
  W2 writer
  C3 closer
  C4 closer
  writechain.closeFn
`, "\n"+tc.Chain.Dump())
}

// TestChain_PrependWriterOrErr_Error tests that PrependWriterOrErr can finish with an error.
func TestChain_PrependWriterOrErr_Error(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)
	ok, err := tc.Chain.PrependWriterOrErr(func(w Writer) (io.Writer, error) {
		return nil, errors.New("some error")
	})
	assert.False(t, ok)
	if assert.Error(t, err) {
		assert.Equal(t, "some error", err.Error())
	}
}

// TestChain_Complex_Ok test operation of a complex Chain.
func TestChain_Complex_Ok(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	tc.SetupComplexChain()

	// Flush
	tc.WriteData([]string{"foo", "bar"})
	tc.AssertFileContent("")
	assert.NoError(t, tc.Chain.Flush())
	tc.AssertFileContent("foobar")

	// Sync
	tc.WriteData([]string{"123", "456"})
	tc.AssertFileContent("foobar")
	assert.NoError(t, tc.Chain.Sync())
	tc.AssertFileContent("foobar123456")

	// Close
	tc.WriteData([]string{"abc", "def"})
	tc.AssertFileContent("foobar123456")
	assert.NoError(t, tc.Chain.Close())
	tc.AssertFileContent("foobar123456abcdef")

	// Check logs
	tc.AssertLogs(`
INFO  TEST: write "foo" to writer "simple"
INFO  TEST: write "foo" to writer "flusher-closer"
INFO  TEST: write "foo" to writer "flusher"
INFO  TEST: write "foo" to writer "closer"
INFO  TEST: write "foo" to writer "buffer1"
INFO  TEST: write string "bar" to writer "simple"
INFO  TEST: write "bar" to writer "flusher-closer"
INFO  TEST: write "bar" to writer "flusher"
INFO  TEST: write "bar" to writer "closer"
INFO  TEST: write "bar" to writer "buffer1"
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
INFO  TEST: flush writer "flusher"
INFO  TEST: flush "func"
INFO  TEST: flush writer "buffer1"
INFO  TEST: write "foobar" to writer "buffer2"
INFO  TEST: flush writer "buffer2"
INFO  TEST: write "foobar" to writer "last"
INFO  TEST: write "foobar" to file
DEBUG  writers flushed
INFO  TEST: write "123" to writer "simple"
INFO  TEST: write "123" to writer "flusher-closer"
INFO  TEST: write "123" to writer "flusher"
INFO  TEST: write "123" to writer "closer"
INFO  TEST: write "123" to writer "buffer1"
INFO  TEST: write string "456" to writer "simple"
INFO  TEST: write "456" to writer "flusher-closer"
INFO  TEST: write "456" to writer "flusher"
INFO  TEST: write "456" to writer "closer"
INFO  TEST: write "456" to writer "buffer1"
DEBUG  syncing file
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
INFO  TEST: flush writer "flusher"
INFO  TEST: flush "func"
INFO  TEST: flush writer "buffer1"
INFO  TEST: write "123456" to writer "buffer2"
INFO  TEST: flush writer "buffer2"
INFO  TEST: write "123456" to writer "last"
INFO  TEST: write "123456" to file
DEBUG  writers flushed
DEBUG  syncing file
INFO  TEST: sync file
DEBUG  file synced
INFO  TEST: write "abc" to writer "simple"
INFO  TEST: write "abc" to writer "flusher-closer"
INFO  TEST: write "abc" to writer "flusher"
INFO  TEST: write "abc" to writer "closer"
INFO  TEST: write "abc" to writer "buffer1"
INFO  TEST: write string "def" to writer "simple"
INFO  TEST: write "def" to writer "flusher-closer"
INFO  TEST: write "def" to writer "flusher"
INFO  TEST: write "def" to writer "closer"
INFO  TEST: write "def" to writer "buffer1"
DEBUG  closing chain
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
INFO  TEST: flush writer "flusher"
INFO  TEST: flush "func"
INFO  TEST: flush writer "buffer1"
INFO  TEST: write "abcdef" to writer "buffer2"
INFO  TEST: flush writer "buffer2"
INFO  TEST: write "abcdef" to writer "last"
INFO  TEST: write "abcdef" to file
DEBUG  writers flushed
INFO  TEST: close writer "flusher-closer"
INFO  TEST: close writer "closer"
INFO  TEST: close "func"
DEBUG  syncing file
INFO  TEST: sync file
DEBUG  file synced
INFO  TEST: close file
DEBUG  chain closed
`)
}

// TestChain_FlushError tests a flusher error.
func TestChain_FlushError(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	writers := tc.SetupSimpleChain()
	writers.FlusherCloser.FlushError = errors.New("flush error")

	tc.WriteData([]string{"foo", "bar"})

	// Flush
	err := tc.Chain.Flush()
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
chain flush error:
- cannot flush "flusher-closer writer": flush error
`), err.Error())
	}

	// Sync
	err = tc.Chain.Sync()
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
chain sync error:
- chain flush error:
  - cannot flush "flusher-closer writer": flush error
`), err.Error())
	}

	// Close
	err = tc.Chain.Close()
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
chain close error:
- chain flush error:
  - cannot flush "flusher-closer writer": flush error
`), err.Error())
	}

	// Check logs
	tc.AssertLogs(`
%A
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
ERROR  cannot flush "flusher-closer writer": flush error
INFO  TEST: flush writer "buffer"
INFO  TEST: write "foobar" to file
DEBUG  writers flushed
DEBUG  syncing file
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
ERROR  cannot flush "flusher-closer writer": flush error
INFO  TEST: flush writer "buffer"
DEBUG  writers flushed
DEBUG  syncing file
INFO  TEST: sync file
DEBUG  file synced
DEBUG  closing chain
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
ERROR  cannot flush "flusher-closer writer": flush error
INFO  TEST: flush writer "buffer"
DEBUG  writers flushed
INFO  TEST: close writer "flusher-closer"
DEBUG  syncing file
INFO  TEST: sync file
DEBUG  file synced
INFO  TEST: close file
DEBUG  chain closed
`)
}

// TestChain_CloseError tests a closer error.
func TestChain_CloseError(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	writers := tc.SetupSimpleChain()
	writers.FlusherCloser.CloseError = errors.New("some close error")

	tc.WriteData([]string{"foo", "bar"})

	// Close
	err := tc.Chain.Close()
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
chain close error:
- cannot close "flusher-closer writer": some close error
`), err.Error())
	}

	// Check logs
	tc.AssertLogs(`
%A
DEBUG  closing chain
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
INFO  TEST: flush writer "buffer"
INFO  TEST: write "foobar" to file
DEBUG  writers flushed
INFO  TEST: close writer "flusher-closer"
ERROR  cannot close "flusher-closer writer": some close error
DEBUG  syncing file
INFO  TEST: sync file
DEBUG  file synced
INFO  TEST: close file
DEBUG  chain closed
`)
}

// TestChain_FileSyncError test an errror reported by the File.Sync().
func TestChain_FileSyncError(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	tc.File.SyncError = errors.New("file sync error")

	tc.SetupSimpleChain()

	tc.WriteData([]string{"foo", "bar"})

	// Sync
	err := tc.Chain.Sync()
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`chain sync error: cannot sync file: file sync error`), err.Error())
	}

	// Close
	err = tc.Chain.Close()
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`chain close error: cannot sync file: file sync error`), err.Error())
	}

	// Check logs
	tc.AssertLogs(`
%A
DEBUG  syncing file
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
INFO  TEST: flush writer "buffer"
INFO  TEST: write "foobar" to file
DEBUG  writers flushed
DEBUG  syncing file
INFO  TEST: sync file
DEBUG  cannot sync file: file sync error
DEBUG  closing chain
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
INFO  TEST: flush writer "buffer"
DEBUG  writers flushed
INFO  TEST: close writer "flusher-closer"
DEBUG  syncing file
INFO  TEST: sync file
DEBUG  cannot sync file: file sync error
INFO  TEST: close file
DEBUG  chain closed
`)
}

// TestChain_FileCloseError tests an error reported by the File.Close().
func TestChain_FileCloseError(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)

	tc.File.CloseError = errors.New("file close error")

	tc.SetupSimpleChain()

	tc.WriteData([]string{"foo", "bar"})

	// Close
	err := tc.Chain.Close()
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`chain close error: cannot close file: file close error`), err.Error())
	}

	// Check logs
	tc.AssertLogs(`
%A
DEBUG  closing chain
DEBUG  flushing writers
INFO  TEST: flush writer "flusher-closer"
INFO  TEST: flush writer "buffer"
INFO  TEST: write "foobar" to file
DEBUG  writers flushed
INFO  TEST: close writer "flusher-closer"
DEBUG  syncing file
INFO  TEST: sync file
DEBUG  file synced
INFO  TEST: close file
ERROR  cannot close file: file close error
DEBUG  chain closed
`)
}

type testFile struct {
	OsFile     *os.File
	Logger     log.Logger
	SyncError  error
	CloseError error
}

type testBuffer struct {
	Buffer *bufio.Writer
	Name   string
	Logger log.Logger
}

type testWriterSimple struct {
	Name   string
	Writer Writer
	Logger log.Logger
}

type testWriterFlusher struct {
	Name       string
	Writer     Writer
	Logger     log.Logger
	FlushError error
}

type testWriterCloser struct {
	Name       string
	Writer     Writer
	Logger     log.Logger
	CloseError error
}

type testWriterFlusherCloser struct {
	Name       string
	Writer     Writer
	Logger     log.Logger
	FlushError error
	CloseError error
}

type testFlusher struct {
	Name       string
	Logger     log.Logger
	FlushError error
}

type testCloser struct {
	Name       string
	Logger     log.Logger
	CloseError error
}

func (w *testFile) Write(p []byte) (int, error) {
	w.Logger.Infof(`TEST: write "%s" to file`, string(p))
	return w.OsFile.Write(p)
}

func (w *testFile) WriteString(s string) (int, error) {
	w.Logger.Infof(`TEST: write string "%s" to file`, s)
	return w.OsFile.WriteString(s)
}

func (w *testFile) Sync() error {
	w.Logger.Info("TEST: sync file")
	if w.SyncError != nil {
		return w.SyncError
	}
	return w.OsFile.Sync()
}

func (w *testFile) Close() error {
	w.Logger.Info("TEST: close file")
	if w.CloseError != nil {
		return w.CloseError
	}
	return w.OsFile.Close()
}

func (w *testBuffer) String() string {
	return w.Name
}

func (w *testBuffer) Write(p []byte) (int, error) {
	w.Logger.Infof(`TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Buffer.Write(p)
}

func (w *testBuffer) Flush() error {
	w.Logger.Infof(`TEST: flush writer "%s"`, w.Name)
	return w.Buffer.Flush()
}

func (w *testWriterSimple) String() string {
	return w.Name + " writer"
}

func (w *testWriterSimple) Write(p []byte) (int, error) {
	w.Logger.Infof(`TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Writer.Write(p)
}

func (w *testWriterSimple) WriteString(s string) (int, error) {
	w.Logger.Infof(`TEST: write string "%s" to writer "%s"`, s, w.Name)
	return w.Writer.WriteString(s)
}

func (w *testWriterFlusher) String() string {
	return w.Name + " writer"
}

func (w *testWriterFlusher) Write(p []byte) (int, error) {
	w.Logger.Infof(`TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Writer.Write(p)
}

func (w *testWriterFlusher) Flush() error {
	w.Logger.Infof(`TEST: flush writer "%s"`, w.Name)
	return w.FlushError
}

func (w *testWriterCloser) String() string {
	return w.Name + " writer"
}

func (w *testWriterCloser) Write(p []byte) (int, error) {
	w.Logger.Infof(`TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Writer.Write(p)
}

func (w *testWriterCloser) Close() error {
	w.Logger.Infof(`TEST: close writer "%s"`, w.Name)
	return w.CloseError
}

func (w *testWriterFlusherCloser) String() string {
	return w.Name + " writer"
}

func (w *testWriterFlusherCloser) Write(p []byte) (int, error) {
	w.Logger.Infof(`TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Writer.Write(p)
}

func (w *testWriterFlusherCloser) Flush() error {
	w.Logger.Infof(`TEST: flush writer "%s"`, w.Name)
	return w.FlushError
}

func (w *testWriterFlusherCloser) Close() error {
	w.Logger.Infof(`TEST: close writer "%s"`, w.Name)
	return w.CloseError
}

func (w *testFlusher) String() string {
	return w.Name + " flusher"
}

func (w *testFlusher) Flush() error {
	w.Logger.Infof(`TEST: flush writer "%s"`, w.Name)
	return w.FlushError
}

func (w *testCloser) String() string {
	return w.Name + " closer"
}

func (w *testCloser) Close() error {
	w.Logger.Infof(`TEST: close writer "%s"`, w.Name)
	return w.CloseError
}

type chainTestCase struct {
	T      testing.TB
	Logger log.DebugLogger
	Path   string
	File   *testFile
	Chain  *Chain
}

type complexChain struct {
	Simple        *testWriterSimple
	Flusher       *testWriterFlusher
	Closer        *testWriterCloser
	FlusherCloser *testWriterFlusherCloser
	Buffer1       *testBuffer
	Buffer2       *testBuffer
	Last          *testWriterSimple
}

type simpleChain struct {
	Simple        *testWriterSimple
	FlusherCloser *testWriterFlusherCloser
	Buffer        *testBuffer
}

func newChainTestCase(t testing.TB) *chainTestCase {
	t.Helper()

	logger := log.NewDebugLogger()
	path := filepath.Join(t.TempDir(), "file")
	osFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	require.NoError(t, err)

	file := &testFile{OsFile: osFile, Logger: logger}
	chain := New(logger, file)

	return &chainTestCase{T: t, Logger: logger, Path: path, File: file, Chain: chain}
}

func (tc *chainTestCase) AssertFileContent(expected string) {
	content, err := os.ReadFile(tc.Path)
	if assert.NoError(tc.T, err) {
		assert.Equal(tc.T, expected, string(content))
	}
}

func (tc *chainTestCase) AssertLogs(expected string) bool {
	return wildcards.Assert(tc.T, strings.TrimSpace(expected), strings.TrimSpace(tc.Logger.AllMessages()))
}

// WriteData writes alternately using Write and WriteString methods.
func (tc *chainTestCase) WriteData(items []string) {
	for i, str := range items {
		if i%2 == 0 {
			n, err := tc.Chain.Write([]byte(str))
			assert.Equal(tc.T, 3, n)
			assert.NoError(tc.T, err)
		} else {
			n, err := tc.Chain.WriteString(str)
			assert.Equal(tc.T, 3, n)
			assert.NoError(tc.T, err)
		}
	}
}

// SetupSimpleChain creates following chain:
// simple -> flusher-closer -> buffer -> file
func (tc *chainTestCase) SetupSimpleChain() *simpleChain {
	out := &simpleChain{}
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.Buffer = &testBuffer{Name: "buffer", Buffer: bufio.NewWriter(w), Logger: tc.Logger}
		return out.Buffer
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.FlusherCloser = &testWriterFlusherCloser{Name: "flusher-closer", Writer: w, Logger: tc.Logger}
		return out.FlusherCloser
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.Simple = &testWriterSimple{Name: "simple", Writer: w, Logger: tc.Logger}
		return out.Simple
	})

	assert.Equal(tc.T, `
Writers:
  simple writer
  flusher-closer writer
  buffer

Flushers:
  flusher-closer writer
  buffer

Closers:
  flusher-closer writer
`, "\n"+tc.Chain.Dump())

	return out
}

// SetupComplexChain creates following chain:
// simple -> flusher-closer -> flusher -> closer -> flush func -> close func -> buffer1 -> buffer2 -> last -> file
func (tc *chainTestCase) SetupComplexChain() *complexChain {
	out := &complexChain{}
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.Last = &testWriterSimple{Name: "last", Writer: w, Logger: tc.Logger}
		return out.Last
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.Buffer2 = &testBuffer{Name: "buffer2", Buffer: bufio.NewWriter(w), Logger: tc.Logger}
		return out.Buffer2
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.Buffer1 = &testBuffer{Name: "buffer1", Buffer: bufio.NewWriter(w), Logger: tc.Logger}
		return out.Buffer1
	})
	tc.Chain.PrependCloseFn(func() error {
		tc.Logger.Info(`TEST: close "func"`)
		return nil
	})
	tc.Chain.PrependFlushFn(func() error {
		tc.Logger.Info(`TEST: flush "func"`)
		return nil
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		// nop
		return w
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.Closer = &testWriterCloser{Name: "closer", Writer: w, Logger: tc.Logger}
		return out.Closer
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.Flusher = &testWriterFlusher{Name: "flusher", Writer: w, Logger: tc.Logger}
		return out.Flusher
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.FlusherCloser = &testWriterFlusherCloser{Name: "flusher-closer", Writer: w, Logger: tc.Logger}
		return out.FlusherCloser
	})
	tc.Chain.PrependWriter(func(w Writer) io.Writer {
		out.Simple = &testWriterSimple{Name: "simple", Writer: w, Logger: tc.Logger}
		return out.Simple
	})

	assert.Equal(tc.T, `
Writers:
  simple writer
  flusher-closer writer
  flusher writer
  closer writer
  buffer1
  buffer2
  last writer

Flushers:
  flusher-closer writer
  flusher writer
  writechain.flushFn
  buffer1
  buffer2

Closers:
  flusher-closer writer
  closer writer
  writechain.closeFn
`, "\n"+tc.Chain.Dump())

	return out
}
