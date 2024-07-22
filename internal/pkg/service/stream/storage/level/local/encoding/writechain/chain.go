// Package writechain provides chain of writers at the end of which is a file.
package writechain

import (
	"context"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Chain of writers at the end of which is the File.
//   - Writer must implement io.Writer interface - "Write([]byte) (int, error)" method.
//   - Writer may implement io.Closer interface - "Close() error" method.
//   - Writer may implement "Flush() error" method to flush internal buffers before the Sync and the Close.
//
// Add a writer:
//   - Use PrependWriter or PrependWriterOrErr methods to add a writer to the beginning of the Chain.
//   - If the writer has a Flush method, PrependFlusher is automatically called too.
//   - If the writer has a Close method, PrependCloser is automatically called too.
//
// Add additional flusher or closer:
//   - If you have a separate flusher or closer, you can add it using one of the following methods:
//   - AppendFlusherCloser
//   - PrependFlusherCloser
//   - AppendFlushFn
//   - PrependFlushFn
//   - AppendCloseFn
//   - PrependCloseFn
//
// Overview of the Chain methods:
//   - Write and WriteString methods perform write through the entire chain to the File at the end.
//   - Sync method performs the Flush (see bellow) and then the File.Sync().
//   - Flush method calls the Flush method on all flushers in the Chain.
//   - Close method calls the Close method on all closers in the Chain and then the File.Close().
type Chain struct {
	logger log.Logger
	// beginning contains the first writer in the chain
	beginning io.Writer
	// writers - list of writers in the chain, before the file.
	writers []*safeWriter
	// flushers - list of resources which must be flushed before the File.Sync.
	flushers []flusher
	// closers - list of resources which must be closed before the File.Close.
	closers []io.Closer
}

func New(logger log.Logger, end io.Writer) *Chain {
	c := &Chain{logger: logger}
	c.PrependWriter(func(_ io.Writer) io.Writer { return end })
	return c
}

// Write to the Chain beginning.
func (c *Chain) Write(p []byte) (n int, err error) {
	return c.beginning.Write(p)
}

// Flush data from writers internal buffers.
func (c *Chain) Flush(ctx context.Context) error {
	c.logger.Debug(ctx, "flushing writers")
	errs := errors.NewMultiError()

	for _, item := range c.flushers {
		if err := item.Flush(); err != nil {
			err = errors.Errorf(`cannot flush "%s": %w`, stringOrType(item), err)
			c.logger.Error(ctx, err.Error())
			errs.Append(err)
		}
	}

	c.logger.Debug(ctx, "writers flushed")

	if err := errs.ErrorOrNil(); err != nil {
		return errors.PrefixError(err, "chain flush error")
	}

	return nil
}

// Close method flushes and closes all writers in the Chain and finally the underlying file.
func (c *Chain) Close(ctx context.Context) error {
	c.logger.Debug(ctx, "closing chain")
	errs := errors.NewMultiError()

	// Close all writers in the chain before the underlying file
	for _, item := range c.closers {
		if err := item.Close(); err != nil {
			err = errors.Errorf(`cannot close "%s": %w`, stringOrType(item), err)
			c.logger.Error(ctx, err.Error())
			errs.Append(err)
		}
	}

	c.logger.Debug(ctx, "chain closed")

	if err := errs.ErrorOrNil(); err != nil {
		return errors.PrefixError(err, "chain close error")
	}

	return nil
}

// PrependWriter method adds writer from the factory to the Chain beginning.
// The factory can return the original writer without changes.
// If the writer implements Flush or Close method, they are automatically registered.
func (c *Chain) PrependWriter(factory func(w io.Writer) io.Writer) (ok bool) {
	ok, _ = c.PrependWriterOrErr(func(w io.Writer) (io.Writer, error) {
		return factory(w), nil
	})
	return ok
}

// PrependWriterOrErr method adds writer from the factory to the Chain beginning.
// The factory can return the original writer without changes.
// If the writer implements Flush or Close method, they are automatically registered.
func (c *Chain) PrependWriterOrErr(factory func(w io.Writer) (io.Writer, error)) (ok bool, err error) {
	// Wrap Chain with the new writer
	oldWriter := c.beginning
	newWriter, err := factory(oldWriter)
	if err != nil {
		return false, err
	}

	// Factory can return nil or oldWriter - it means no operation.
	same := newWriter == nil || newWriter == oldWriter

	if !same {
		// Protect asynchronous Flush with a lock, add WriteString method if needed
		safe := newSafeWriter(newWriter)

		c.writers = append([]*safeWriter{safe}, c.writers...)

		// Register flusher for periodical sync
		if _, ok := newWriter.(flusher); ok {
			c.addFlusher(true, safe)
		}

		// Register closer: use Close method if exists, or call Flush also on Close
		if v, ok := newWriter.(io.Closer); ok {
			c.addCloser(true, v)
		} else if _, ok := newWriter.(flusher); ok {
			c.addCloser(true, newCloseFn(newWriter, safe.Flush))
		}

		c.beginning = safe
	}

	return !same, nil
}

// AppendFlusherCloser adds
// the Flush method if v implements it,
// the Close method if v implements it,
// to the Chain end.
// At least one method must be implemented.
func (c *Chain) AppendFlusherCloser(v any) {
	c.addFlusherCloser(false, v)
}

// PrependFlusherCloser adds
// the Flush method if v implements it,
// the Close method if v implements it,
// to the Chain beginning.
// At least one method must be implemented.
func (c *Chain) PrependFlusherCloser(v any) {
	c.addFlusherCloser(true, v)
}

// AppendFlushFn adds the Flush function to the Chain end.
// Info is a value used for identification of the function in the Chain.Dump, for example a related structure.
func (c *Chain) AppendFlushFn(info any, fn func() error) {
	c.addFlusher(false, newFlushFn(info, fn))
	c.addCloser(false, newCloseFn(info, fn))
}

// PrependFlushFn adds the Flush function to the Chain beginning.
// Info is a value used for identification of the function in the Chain.Dump, for example a related structure.
func (c *Chain) PrependFlushFn(info any, fn func() error) {
	c.addFlusher(true, newFlushFn(info, fn))
	c.addCloser(true, newCloseFn(info, fn))
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

func (c *Chain) addFlusherCloser(prepend bool, v any) {
	vf, isFlusher := v.(flusher)
	vc, isCloser := v.(io.Closer)
	if !isFlusher && !isCloser {
		panic(errors.Errorf(`type "%T" must have Flush or/and Close method`, v))
	}

	// Register flusher for periodical sync
	if isFlusher {
		c.addFlusher(prepend, vf)
	}

	// Register closer: use Close method if exists, or call Flush also on Close
	if isCloser {
		c.addCloser(prepend, vc)
	} else {
		c.addCloser(prepend, newCloseFn(v, vf.Flush))
	}
}

func (c *Chain) addFlusher(prepend bool, v flusher) {
	if prepend {
		c.flushers = append([]flusher{v}, c.flushers...)
	} else {
		c.flushers = append(c.flushers, v)
	}
}

func (c *Chain) addCloser(prepend bool, v io.Closer) {
	if prepend {
		c.closers = append([]io.Closer{v}, c.closers...)
	} else {
		c.closers = append(c.closers, v)
	}
}
