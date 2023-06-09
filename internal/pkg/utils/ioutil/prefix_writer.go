package ioutil

import (
	"io"

	"github.com/umisama/go-regexpcache"
)

type prefixWriter struct {
	prefix   string
	prefixNL string
	writer   io.Writer
}

func NewPrefixWriter(prefix string, writer io.Writer) io.Writer {
	return &prefixWriter{prefix: prefix, prefixNL: "\n" + prefix + "$1", writer: writer}
}

func (w *prefixWriter) Write(p []byte) (n int, err error) {
	l := len(p)
	pStr := string(p)
	pStr = w.prefix + regexpcache.MustCompile(`\n(.)`).ReplaceAllString(pStr, w.prefixNL)
	if _, err := w.writer.Write([]byte(pStr)); err != nil {
		return 0, err
	}
	return l, nil
}
