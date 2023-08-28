package writechain

import "io"

// stringWriter adds WriteString method to a writer without it.
type stringWriter struct {
	io.Writer
}

func (f *stringWriter) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}
