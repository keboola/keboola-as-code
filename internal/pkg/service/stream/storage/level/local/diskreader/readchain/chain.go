// Package readchain provides chain of readers with support for the Close method.
package readchain

import (
	"context"
	"io"
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Chain of readers with support for the Close method.
//   - Reader must implement io.Reader interface - "Read([]byte) (int, error)" method.
//   - Reader may implement io.Closer interface - "Close() error" method.
//
// Add a Reader:
//   - Use PrependReader or PrependReaderOrErr methods to add a reader to the beginning of the Chain.
//   - If the reader has a Close method, PrependCloser is automatically called too.
//
// Add additional closer:
//   - If you have a separate closer, you can add it using one of the following methods:
//   - AppendCloser
//   - PrependCloser
//   - AppendCloseFn
//   - PrependCloseFn
type Chain struct {
	logger log.Logger
	// file at the end of the chain
	file File
	// beginning contains the first reader in the chain
	beginning io.Reader
	// readers - list of readers in the chain.
	readers []io.Reader
	// closers - list of resources which must be closed on Close.
	closers []io.Closer
}

// File defines used method from the *os.File.
// It makes it possible to mock the file in the tests.
type File interface {
	io.Reader
	Close() error
}

func New(logger log.Logger, file File) *Chain {
	return &Chain{logger: logger, file: file, beginning: file}
}

// UnwrapFile returns underlying file, if it is the only element in the chain.
//
// This is preferred way, it enables internal Go optimization and "zero CPU copy" to be used,
// read more about "sendfile" syscall for details.
//
// The Close methods should always be called on the Chain, not directly on the File,
// because Chain may contain multiple closers, even if there is only one reader.
func (c *Chain) UnwrapFile() (file *os.File, ok bool) {
	if len(c.readers) == 0 {
		if f, ok := c.beginning.(*os.File); ok {
			return f, true
		}
	}
	return nil, false
}

// PrependReader method adds reader from the factory to the Chain beginning.
// The factory can return the original reader without changes.
// If the reader implements Close method, it is automatically registered.
func (c *Chain) PrependReader(factory func(io.Reader) io.Reader) (ok bool) {
	ok, _ = c.PrependReaderOrErr(func(r io.Reader) (io.Reader, error) {
		return factory(r), nil
	})
	return ok
}

// PrependReaderOrErr method adds reader from the factory to the Chain beginning.
// The factory can return the original reader without changes.
// If the reader implements Close method, it is automatically registered.
func (c *Chain) PrependReaderOrErr(factory func(io.Reader) (io.Reader, error)) (ok bool, err error) {
	// Wrap Chain with the new reader
	oldReader := c.beginning
	newReader, err := factory(oldReader)
	if err != nil {
		return false, err
	}

	// Factory can return nil or oldReader - it means no operation.
	same := newReader == nil || newReader == oldReader

	if !same {
		c.readers = append([]io.Reader{newReader}, c.readers...)

		// Register close method
		if v, ok := newReader.(io.Closer); ok {
			c.addCloser(true, v)
		}

		c.beginning = newReader
	}

	return !same, nil
}

// Read from the Chain beginning.
func (c *Chain) Read(p []byte) (n int, err error) {
	return c.beginning.Read(p)
}

// Close method flushes and closes all readers in the Chain and finally the underlying file.
func (c *Chain) Close(ctx context.Context) error {
	c.logger.Debug(ctx, "closing chain")
	errs := errors.NewMultiError()

	// Close all reader in the chain
	for _, item := range c.closers {
		if err := item.Close(); err != nil {
			err = errors.PrefixErrorf(err, `cannot close "%s"`, stringOrType(item))
			c.logger.Error(ctx, err.Error())
			errs.Append(err)
		}
	}

	// Close the underlying file
	if err := c.file.Close(); err != nil {
		err = errors.PrefixError(err, `cannot close file`)
		c.logger.Error(ctx, err.Error())
		errs.Append(err)
	}

	c.logger.Debug(ctx, "chain closed")

	if err := errs.ErrorOrNil(); err != nil {
		return errors.PrefixError(err, "chain close error")
	}

	return nil
}

// AppendCloser adds an io.Closer to the Chain end.
func (c *Chain) AppendCloser(v io.Closer) {
	c.addCloser(false, v)
}

// PrependCloser adds an io.Closer to the Chain beginning.
func (c *Chain) PrependCloser(v io.Closer) {
	c.addCloser(true, v)
}

// AppendCloseFn adds the Close function to the Chain end.
// Info is a value used for identification of the function in the Chain.Dump, for example a related structure.
func (c *Chain) AppendCloseFn(info any, fn func() error) {
	c.addCloser(false, newCloseFn(info, fn))
}

// PrependCloseFn adds the Close function to the Chain beginning.
// Info is a value used for identification of the function in the Chain.Dump, for example a related structure.
func (c *Chain) PrependCloseFn(info any, fn func() error) {
	c.addCloser(true, newCloseFn(info, fn))
}

func (c *Chain) addCloser(prepend bool, v io.Closer) {
	if prepend {
		c.closers = append([]io.Closer{v}, c.closers...)
	} else {
		c.closers = append(c.closers, v)
	}
}
