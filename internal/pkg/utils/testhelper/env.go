// nolint forbidigo
package testhelper

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/acarl005/stripansi"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type EnvProvider interface {
	MustGet(key string) string
}

// ReplaceEnvsStringWithSeparator replaces ENVs in given string with chosen separator.
func ReplaceEnvsStringWithSeparator(str string, provider EnvProvider, envSeparator string) string {
	return regexp.
		MustCompile(fmt.Sprintf(`%s[a-zA-Z0-9][a-zA-Z0-9\-_]*[a-zA-Z0-9]%s`, envSeparator, envSeparator)).
		ReplaceAllStringFunc(str, func(s string) string {
			return provider.MustGet(strings.Trim(s, envSeparator))
		})
}

// ReplaceEnvsString replaces ENVs in given string.
func ReplaceEnvsString(str string, provider EnvProvider) string {
	return ReplaceEnvsStringWithSeparator(str, provider, "%%")
}

// ReplaceEnvsFileWithSeparator replaces ENVs in given file with chosen separator.
func ReplaceEnvsFileWithSeparator(fs filesystem.Fs, path string, provider EnvProvider, envSeparator string) {
	file, err := fs.ReadFile(filesystem.NewFileDef(path))
	if err != nil {
		panic(err)
	}
	file.Content = ReplaceEnvsStringWithSeparator(file.Content, provider, envSeparator)
	if err := fs.WriteFile(file); err != nil {
		panic(fmt.Errorf("cannot write to file \"%s\": %w", path, err))
	}
}

// ReplaceEnvsFile replaces ENVs in given file.
func ReplaceEnvsFile(fs filesystem.Fs, path string, provider EnvProvider) {
	ReplaceEnvsFileWithSeparator(fs, path, provider, "%%")
}

// ReplaceEnvsDirWithSeparator replaces ENVs in all files in root directory and sub-directories withc chosen separator.
func ReplaceEnvsDirWithSeparator(fs filesystem.Fs, root string, provider EnvProvider, envSeparator string) {
	// Iterate over directory structure
	err := fs.Walk(root, func(path string, info filesystem.FileInfo, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore hidden files, except .env*, .gitignore
		if IsIgnoredFile(path, info) {
			return nil
		}

		// Process file
		if !info.IsDir() {
			ReplaceEnvsFileWithSeparator(fs, path, provider, envSeparator)
		}

		return nil
	})
	if err != nil {
		panic(fmt.Errorf("cannot walk over dir \"%s\": %w", root, err))
	}
}

// ReplaceEnvsDir replaces ENVs in all files in root directory and sub-directories.
func ReplaceEnvsDir(fs filesystem.Fs, root string, provider EnvProvider) {
	ReplaceEnvsDirWithSeparator(fs, root, provider, "%%")
}

// stripAnsiWriter strips ANSI characters from
type stripAnsiWriter struct {
	buf    *bytes.Buffer
	writer io.Writer
}

func newStripAnsiWriter(writer io.Writer) *stripAnsiWriter {
	return &stripAnsiWriter{
		buf:    &bytes.Buffer{},
		writer: writer,
	}
}

func (w *stripAnsiWriter) writeBuffer() error {
	if _, err := w.writer.Write([]byte(stripansi.Strip(w.buf.String()))); err != nil {
		return err
	}
	w.buf.Reset()
	return nil
}

func (w *stripAnsiWriter) Write(p []byte) (int, error) {
	// Append to the buffer
	n, err := w.buf.Write(p)

	// We can only remove an ANSI escape seq if the whole expression is present.
	// ... so if buffer contains new line -> flush
	if bytes.Contains(w.buf.Bytes(), []byte("\n")) {
		if err := w.writeBuffer(); err != nil {
			return 0, err
		}
	}

	return n, err
}

func (w *stripAnsiWriter) Close() error {
	if err := w.writeBuffer(); err != nil {
		return err
	}
	return nil
}

type nopCloser struct {
	io.Writer
}

func (n *nopCloser) Close() error {
	return nil
}

func TestIsVerbose() bool {
	value := os.Getenv("TEST_VERBOSE")
	if value == "" {
		value = "false"
	}
	return cast.ToBool(value)
}

func VerboseStdout() io.WriteCloser {
	if TestIsVerbose() {
		return newStripAnsiWriter(os.Stdout)
	}

	return &nopCloser{io.Discard}
}

func VerboseStderr() io.WriteCloser {
	if TestIsVerbose() {
		return newStripAnsiWriter(os.Stderr)
	}

	return &nopCloser{io.Discard}
}
