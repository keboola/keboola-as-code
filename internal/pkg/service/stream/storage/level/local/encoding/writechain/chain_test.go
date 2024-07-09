package writechain

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	assert.True(t, tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		return &testWriterSimple{Writer: w, Name: "W1"}
	}))

	assert.False(t, tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		return w
	}))

	assert.False(t, tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		return nil
	}))

	ok, err := tc.Chain.PrependWriterOrErr(func(w io.Writer) (io.Writer, error) {
		return &testWriterFlusherCloser{Writer: w, Name: "W2"}, nil
	})
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = tc.Chain.PrependWriterOrErr(func(w io.Writer) (io.Writer, error) {
		return w, nil
	})
	assert.False(t, ok)
	assert.NoError(t, err)

	ok, err = tc.Chain.PrependWriterOrErr(func(w io.Writer) (io.Writer, error) {
		return nil, nil
	})
	assert.False(t, ok)
	assert.NoError(t, err)

	// Flushers
	tc.Chain.PrependFlusherCloser(&testFlusher{Name: "F1"})
	tc.Chain.PrependFlusherCloser(&testFlusher{Name: "F2"})
	tc.Chain.PrependFlushFn("fn1", func() error { return nil })
	tc.Chain.AppendFlusherCloser(&testFlusher{Name: "F3"})
	tc.Chain.AppendFlusherCloser(&testFlusher{Name: "F4"})
	tc.Chain.AppendFlushFn("fn2", func() error { return nil })

	// Closers
	tc.Chain.PrependFlusherCloser(&testCloser{Name: "C1"})
	tc.Chain.PrependFlusherCloser(&testCloser{Name: "C2"})
	tc.Chain.PrependCloseFn("fn3", func() error { return nil })
	tc.Chain.AppendFlusherCloser(&testCloser{Name: "C3"})
	tc.Chain.AppendFlusherCloser(&testCloser{Name: "C4"})
	tc.Chain.AppendCloseFn("fn4", func() error { return nil })

	// Flusher + Closers
	tc.Chain.PrependFlusherCloser(&testFlusherCloser{Name: "FC1"})
	tc.Chain.AppendFlusherCloser(&testFlusherCloser{Name: "FC2"})

	// Invalid type
	assert.PanicsWithError(t, `type "string" must have Flush or/and Close method`, func() {
		tc.Chain.PrependFlusherCloser("invalid type")
	})
	assert.PanicsWithError(t, `type "string" must have Flush or/and Close method`, func() {
		tc.Chain.AppendFlusherCloser("invalid type")
	})

	assert.Equal(t, `
Writers:
  W2 writer
  W1 writer

Flushers:
  FC1 flusher closer
  fn1
  F2 flusher
  F1 flusher
  W2 writer
  F3 flusher
  F4 flusher
  fn2
  FC2 flusher closer

Closers:
  FC1 flusher closer
  fn3
  C2 closer
  C1 closer
  fn1
  F2 flusher
  F1 flusher
  W2 writer
  F3 flusher
  F4 flusher
  fn2
  C3 closer
  C4 closer
  fn4
  FC2 flusher closer
`, "\n"+tc.Chain.Dump())
}

// TestChain_PrependWriterOrErr_Error tests that PrependWriterOrErr can finish with an error.
func TestChain_PrependWriterOrErr_Error(t *testing.T) {
	t.Parallel()
	tc := newChainTestCase(t)
	ok, err := tc.Chain.PrependWriterOrErr(func(w io.Writer) (io.Writer, error) {
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

	ctx := context.Background()
	tc := newChainTestCase(t)
	tc.SetupComplexChain()

	// Flush
	tc.WriteData([]string{"foo", "bar"})
	tc.AssertFileContent("")
	assert.NoError(t, tc.Chain.Flush(ctx))
	tc.AssertFileContent("foobar")

	// Sync
	tc.WriteData([]string{"123", "456"})
	tc.AssertFileContent("foobar")
	assert.NoError(t, tc.Chain.Sync(ctx))
	tc.AssertFileContent("foobar123456")

	// Close
	tc.WriteData([]string{"abc", "def"})
	tc.AssertFileContent("foobar123456")
	assert.NoError(t, tc.Chain.Close(ctx))
	tc.AssertFileContent("foobar123456abcdef")

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"TEST: write \"foo\" to writer \"simple\""}                       
{"level":"info","message":"TEST: write \"foo\" to writer \"flusher-closer\""}               
{"level":"info","message":"TEST: write \"foo\" to writer \"flusher\""}                      
{"level":"info","message":"TEST: write \"foo\" to writer \"closer\""}                       
{"level":"info","message":"TEST: write \"foo\" to writer \"buffer1\""}                      
{"level":"info","message":"TEST: write \"bar\" to writer \"simple\""}                
{"level":"info","message":"TEST: write \"bar\" to writer \"flusher-closer\""}               
{"level":"info","message":"TEST: write \"bar\" to writer \"flusher\""}                      
{"level":"info","message":"TEST: write \"bar\" to writer \"closer\""}                       
{"level":"info","message":"TEST: write \"bar\" to writer \"buffer1\""}                      
{"level":"debug","message":"flushing writers"}              
{"level":"info","message":"TEST: flush writer \"flusher-closer\""}                          
{"level":"info","message":"TEST: flush writer \"flusher\""} 
{"level":"info","message":"TEST: flush \"func\""}
{"level":"info","message":"TEST: flush writer \"buffer1\""}
{"level":"info","message":"TEST: write \"foobar\" to writer \"buffer2\""}
{"level":"info","message":"TEST: flush writer \"buffer2\""}
{"level":"info","message":"TEST: write \"foobar\" to writer \"last\""}
{"level":"info","message":"TEST: write \"foobar\" to file"}
{"level":"debug","message":"writers flushed"}
{"level":"info","message":"TEST: write \"123\" to writer \"simple\""}
{"level":"info","message":"TEST: write \"123\" to writer \"flusher-closer\""}
{"level":"info","message":"TEST: write \"123\" to writer \"flusher\""}
{"level":"info","message":"TEST: write \"123\" to writer \"closer\""}
{"level":"info","message":"TEST: write \"123\" to writer \"buffer1\""}
{"level":"info","message":"TEST: write \"456\" to writer \"simple\""}
{"level":"info","message":"TEST: write \"456\" to writer \"flusher-closer\""}
{"level":"info","message":"TEST: write \"456\" to writer \"flusher\""}
{"level":"info","message":"TEST: write \"456\" to writer \"closer\""}
{"level":"info","message":"TEST: write \"456\" to writer \"buffer1\""}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"flushing writers"}
{"level":"info","message":"TEST: flush writer \"flusher-closer\""}
{"level":"info","message":"TEST: flush writer \"flusher\""}
{"level":"info","message":"TEST: flush \"func\""}
{"level":"info","message":"TEST: flush writer \"buffer1\""}
{"level":"info","message":"TEST: write \"123456\" to writer \"buffer2\""}
{"level":"info","message":"TEST: flush writer \"buffer2\""}
{"level":"info","message":"TEST: write \"123456\" to writer \"last\""}
{"level":"info","message":"TEST: write \"123456\" to file"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"syncing file"}
{"level":"info","message":"TEST: sync file"}
{"level":"debug","message":"file synced"}
{"level":"info","message":"TEST: write \"abc\" to writer \"simple\""}
{"level":"info","message":"TEST: write \"abc\" to writer \"flusher-closer\""}
{"level":"info","message":"TEST: write \"abc\" to writer \"flusher\""}
{"level":"info","message":"TEST: write \"abc\" to writer \"closer\""}
{"level":"info","message":"TEST: write \"abc\" to writer \"buffer1\""}
{"level":"info","message":"TEST: write \"def\" to writer \"simple\""}
{"level":"info","message":"TEST: write \"def\" to writer \"flusher-closer\""}
{"level":"info","message":"TEST: write \"def\" to writer \"flusher\""}
{"level":"info","message":"TEST: write \"def\" to writer \"closer\""}
{"level":"info","message":"TEST: write \"def\" to writer \"buffer1\""}
{"level":"debug","message":"closing chain"}
{"level":"info","message":"TEST: close writer \"flusher-closer\""}
{"level":"info","message":"TEST: flush writer \"flusher\""}
{"level":"info","message":"TEST: close writer \"closer\""}
{"level":"info","message":"TEST: flush \"func\""}
{"level":"info","message":"TEST: close \"func\""}
{"level":"info","message":"TEST: flush writer \"buffer1\""}
{"level":"info","message":"TEST: write \"abcdef\" to writer \"buffer2\""}
{"level":"info","message":"TEST: flush writer \"buffer2\""}
{"level":"info","message":"TEST: write \"abcdef\" to writer \"last\""}
{"level":"info","message":"TEST: write \"abcdef\" to file"}
{"level":"debug","message":"syncing file"}
{"level":"info","message":"TEST: sync file"}
{"level":"debug","message":"file synced"}
{"level":"info","message":"TEST: close file"}
{"level":"debug","message":"chain closed"}
`)
}

// TestChain_FlushError tests a flusher error.
func TestChain_FlushError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newChainTestCase(t)

	writers := tc.SetupSimpleChain()
	writers.FlusherCloser.FlushError = errors.New("flush error")

	tc.WriteData([]string{"foo", "bar"})

	// Flush
	err := tc.Chain.Flush(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
chain flush error:
- cannot flush "flusher-closer writer": flush error
`), err.Error())
	}

	// Sync
	err = tc.Chain.Sync(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
chain sync error:
- chain flush error:
  - cannot flush "flusher-closer writer": flush error
`), err.Error())
	}

	// Close
	assert.NoError(t, tc.Chain.Close(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"flushing writers"}
{"level":"info","message":"TEST: flush writer \"flusher-closer\""}
{"level":"error","message":"cannot flush \"flusher-closer writer\": flush error"}
{"level":"info","message":"TEST: flush writer \"buffer\""}
{"level":"info","message":"TEST: write \"foobar\" to file"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"flushing writers"}
{"level":"info","message":"TEST: flush writer \"flusher-closer\""}
{"level":"error","message":"cannot flush \"flusher-closer writer\": flush error"}
{"level":"info","message":"TEST: flush writer \"buffer\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"syncing file"}
{"level":"info","message":"TEST: sync file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"closing chain"}
{"level":"info","message":"TEST: close writer \"flusher-closer\""}
{"level":"info","message":"TEST: flush writer \"buffer\""}
{"level":"debug","message":"syncing file"}
{"level":"info","message":"TEST: sync file"}
{"level":"debug","message":"file synced"}
{"level":"info","message":"TEST: close file"}
{"level":"debug","message":"chain closed"}
`)
}

// TestChain_CloseError tests a closer error.
func TestChain_CloseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newChainTestCase(t)

	writers := tc.SetupSimpleChain()
	writers.FlusherCloser.CloseError = errors.New("some close error")

	tc.WriteData([]string{"foo", "bar"})

	// Close
	err := tc.Chain.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
chain close error:
- cannot close "flusher-closer writer": some close error
`), err.Error())
	}

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"closing chain"}
{"level":"info","message":"TEST: close writer \"flusher-closer\""}
{"level":"error","message":"cannot close \"flusher-closer writer\": some close error"}
{"level":"info","message":"TEST: flush writer \"buffer\""}
{"level":"info","message":"TEST: write \"foobar\" to file"}
{"level":"debug","message":"syncing file"}
{"level":"info","message":"TEST: sync file"}
{"level":"debug","message":"file synced"}
{"level":"info","message":"TEST: close file"}
{"level":"debug","message":"chain closed"}

`)
}

// TestChain_FileSyncError test an errror reported by the File.Sync().
func TestChain_FileSyncError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newChainTestCase(t)

	tc.File.SyncError = errors.New("file sync error")

	tc.SetupSimpleChain()

	tc.WriteData([]string{"foo", "bar"})

	// Sync
	err := tc.Chain.Sync(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`chain sync error: cannot sync file: file sync error`), err.Error())
	}

	// Close
	err = tc.Chain.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`chain close error: cannot sync file: file sync error`), err.Error())
	}

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"flushing writers"}
{"level":"info","message":"TEST: flush writer \"flusher-closer\""}
{"level":"info","message":"TEST: flush writer \"buffer\""}
{"level":"info","message":"TEST: write \"foobar\" to file"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"syncing file"}
{"level":"info","message":"TEST: sync file"}
{"level":"debug","message":"cannot sync file: file sync error"}
{"level":"debug","message":"closing chain"}
{"level":"info","message":"TEST: close writer \"flusher-closer\""}
{"level":"info","message":"TEST: flush writer \"buffer\""}
{"level":"debug","message":"syncing file"}
{"level":"info","message":"TEST: sync file"}
{"level":"debug","message":"cannot sync file: file sync error"}
{"level":"info","message":"TEST: close file"}
{"level":"debug","message":"chain closed"}
`)
}

// TestChain_FileCloseError tests an error reported by the File.Close().
func TestChain_FileCloseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newChainTestCase(t)

	tc.File.CloseError = errors.New("file close error")

	tc.SetupSimpleChain()

	tc.WriteData([]string{"foo", "bar"})

	// Close
	err := tc.Chain.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`chain close error: cannot close file: file close error`), err.Error())
	}

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"closing chain"}
{"level":"info","message":"TEST: close writer \"flusher-closer\""}
{"level":"info","message":"TEST: flush writer \"buffer\""}
{"level":"info","message":"TEST: write \"foobar\" to file"}
{"level":"debug","message":"syncing file"}
{"level":"info","message":"TEST: sync file"}
{"level":"debug","message":"file synced"}
{"level":"info","message":"TEST: close file"}
{"level":"error","message":"cannot close file: file close error"}
{"level":"debug","message":"chain closed"}
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
	Writer io.Writer
	Logger log.Logger
}

type testWriterFlusher struct {
	Name       string
	Writer     io.Writer
	Logger     log.Logger
	FlushError error
}

type testWriterCloser struct {
	Name       string
	Writer     io.Writer
	Logger     log.Logger
	CloseError error
}

type testWriterFlusherCloser struct {
	Name       string
	Writer     io.Writer
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

type testFlusherCloser struct {
	Name       string
	Logger     log.Logger
	FlushError error
	CloseError error
}

func (w *testFile) Write(p []byte) (int, error) {
	w.Logger.Infof(context.Background(), `TEST: write "%s" to file`, string(p))
	return w.OsFile.Write(p)
}

func (w *testFile) WriteString(s string) (int, error) {
	w.Logger.Infof(context.Background(), `TEST: write "%s" to file`, s)
	return w.OsFile.WriteString(s)
}

func (w *testFile) Sync() error {
	w.Logger.Info(context.Background(), "TEST: sync file")
	if w.SyncError != nil {
		return w.SyncError
	}
	return w.OsFile.Sync()
}

func (w *testFile) Close(ctx context.Context) error {
	w.Logger.Info(ctx, "TEST: close file")
	if w.CloseError != nil {
		return w.CloseError
	}
	return w.OsFile.Close()
}

func (w *testBuffer) String() string {
	return w.Name
}

func (w *testBuffer) Write(p []byte) (int, error) {
	w.Logger.Infof(context.Background(), `TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Buffer.Write(p)
}

func (w *testBuffer) Flush() error {
	w.Logger.Infof(context.Background(), `TEST: flush writer "%s"`, w.Name)
	return w.Buffer.Flush()
}

func (w *testWriterSimple) String() string {
	return w.Name + " writer"
}

func (w *testWriterSimple) Write(p []byte) (int, error) {
	w.Logger.Infof(context.Background(), `TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Writer.Write(p)
}

func (w *testWriterFlusher) String() string {
	return w.Name + " writer"
}

func (w *testWriterFlusher) Write(p []byte) (int, error) {
	w.Logger.Infof(context.Background(), `TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Writer.Write(p)
}

func (w *testWriterFlusher) Flush() error {
	w.Logger.Infof(context.Background(), `TEST: flush writer "%s"`, w.Name)
	return w.FlushError
}

func (w *testWriterCloser) String() string {
	return w.Name + " writer"
}

func (w *testWriterCloser) Write(p []byte) (int, error) {
	w.Logger.Infof(context.Background(), `TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Writer.Write(p)
}

func (w *testWriterCloser) Close() error {
	w.Logger.Infof(context.Background(), `TEST: close writer "%s"`, w.Name)
	return w.CloseError
}

func (w *testWriterFlusherCloser) String() string {
	return w.Name + " writer"
}

func (w *testWriterFlusherCloser) Write(p []byte) (int, error) {
	w.Logger.Infof(context.Background(), `TEST: write "%s" to writer "%s"`, string(p), w.Name)
	return w.Writer.Write(p)
}

func (w *testWriterFlusherCloser) Flush() error {
	w.Logger.Infof(context.Background(), `TEST: flush writer "%s"`, w.Name)
	return w.FlushError
}

func (w *testWriterFlusherCloser) Close() error {
	w.Logger.Infof(context.Background(), `TEST: close writer "%s"`, w.Name)
	return w.CloseError
}

func (w *testFlusher) String() string {
	return w.Name + " flusher"
}

func (w *testFlusher) Flush() error {
	w.Logger.Infof(context.Background(), `TEST: flush writer "%s"`, w.Name)
	return w.FlushError
}

func (w *testCloser) String() string {
	return w.Name + " closer"
}

func (w *testCloser) Close() error {
	w.Logger.Infof(context.Background(), `TEST: close writer "%s"`, w.Name)
	return w.CloseError
}

func (w *testFlusherCloser) String() string {
	return w.Name + " flusher closer"
}

func (w *testFlusherCloser) Flush() error {
	w.Logger.Infof(context.Background(), `TEST: flush "%s"`, w.Name)
	return w.FlushError
}

func (w *testFlusherCloser) Close() error {
	w.Logger.Infof(context.Background(), `TEST: close "%s"`, w.Name)
	return w.CloseError
}

type chainTestCase struct {
	TB     testing.TB
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

func newChainTestCase(tb testing.TB) *chainTestCase {
	tb.Helper()

	logger := log.NewDebugLogger()
	path := filepath.Join(tb.TempDir(), "file")
	osFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	require.NoError(tb, err)

	file := &testFile{OsFile: osFile, Logger: logger}
	chain := New(logger, file)

	return &chainTestCase{TB: tb, Logger: logger, Path: path, File: file, Chain: chain}
}

func (tc *chainTestCase) AssertFileContent(expected string) {
	content, err := os.ReadFile(tc.Path)
	if assert.NoError(tc.TB, err) {
		assert.Equal(tc.TB, expected, string(content))
	}
}

func (tc *chainTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.TB, expected)
}

func (tc *chainTestCase) WriteData(items []string) {
	for _, str := range items {
		n, err := tc.Chain.Write([]byte(str))
		assert.Equal(tc.TB, 3, n)
		assert.NoError(tc.TB, err)
	}
}

// SetupSimpleChain creates following chain:
// simple -> flusher-closer -> buffer -> file.
func (tc *chainTestCase) SetupSimpleChain() *simpleChain {
	out := &simpleChain{}
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.Buffer = &testBuffer{Name: "buffer", Buffer: bufio.NewWriter(w), Logger: tc.Logger}
		return out.Buffer
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.FlusherCloser = &testWriterFlusherCloser{Name: "flusher-closer", Writer: w, Logger: tc.Logger}
		return out.FlusherCloser
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.Simple = &testWriterSimple{Name: "simple", Writer: w, Logger: tc.Logger}
		return out.Simple
	})

	assert.Equal(tc.TB, `
Writers:
  simple writer
  flusher-closer writer
  buffer

Flushers:
  flusher-closer writer
  buffer

Closers:
  flusher-closer writer
  buffer
`, "\n"+tc.Chain.Dump())

	return out
}

// SetupComplexChain creates following chain:
// simple -> flusher-closer -> flusher -> closer -> flush func -> close func -> buffer1 -> buffer2 -> last -> file.
func (tc *chainTestCase) SetupComplexChain() *complexChain {
	out := &complexChain{}
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.Last = &testWriterSimple{Name: "last", Writer: w, Logger: tc.Logger}
		return out.Last
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.Buffer2 = &testBuffer{Name: "buffer2", Buffer: bufio.NewWriter(w), Logger: tc.Logger}
		return out.Buffer2
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.Buffer1 = &testBuffer{Name: "buffer1", Buffer: bufio.NewWriter(w), Logger: tc.Logger}
		return out.Buffer1
	})
	tc.Chain.PrependCloseFn("fn1", func() error {
		tc.Logger.Info(context.Background(), `TEST: close "func"`)
		return nil
	})
	tc.Chain.PrependFlushFn("fn2", func() error {
		tc.Logger.Info(context.Background(), `TEST: flush "func"`)
		return nil
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		// nop
		return w
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.Closer = &testWriterCloser{Name: "closer", Writer: w, Logger: tc.Logger}
		return out.Closer
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.Flusher = &testWriterFlusher{Name: "flusher", Writer: w, Logger: tc.Logger}
		return out.Flusher
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.FlusherCloser = &testWriterFlusherCloser{Name: "flusher-closer", Writer: w, Logger: tc.Logger}
		return out.FlusherCloser
	})
	tc.Chain.PrependWriter(func(w io.Writer) io.Writer {
		out.Simple = &testWriterSimple{Name: "simple", Writer: w, Logger: tc.Logger}
		return out.Simple
	})

	assert.Equal(tc.TB, `
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
  fn2
  buffer1
  buffer2

Closers:
  flusher-closer writer
  flusher writer
  closer writer
  fn2
  fn1
  buffer1
  buffer2
`, "\n"+tc.Chain.Dump())

	return out
}
