// Package download contains the implementation of the "kbc project remote file download" command.
package download

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/schollz/progressbar/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

type Options struct {
	File        *keboola.FileDownloadCredentials
	Output      string
	AllowSliced bool
}

func (o *Options) ToStdOut() bool {
	return o.Output == "-"
}

// GetOutput returns an `io.WriteCloser` which either wraps a file or stdout.
//
// If `o.Output` is "-", then it wraps stdout, and `Close` is a no-op.
// Otherwise wraps a file.
//
// If `file` is not an empty string, then `o.Output` is treated as a directory
// and the file created will be at `path.Join(o.Output, file)`.
func (o *Options) GetOutput(file string) (io.WriteCloser, error) {
	if o.Output == "-" {
		return &outputWriter{}, nil
	} else {
		var output string
		if len(file) > 0 {
			output = path.Join(o.Output, file)
		} else {
			output = o.Output
		}

		file, err := os.Create(output) // nolint: forbidigo
		if err != nil {
			return nil, errors.Errorf(`cannot create file "%s": %w`, output, err)
		}
		return &outputWriter{file}, nil
	}
}

func Run(ctx context.Context, opts Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.file.download")
	defer telemetry.EndSpan(span, &err)

	if opts.File.IsSliced {
		if !opts.AllowSliced {
			err = runDownloadForceUnsliced(ctx, &opts, d.Logger())
			if err != nil {
				return err
			}
		} else {
			err = runDownloadSliced(ctx, &opts)
			if err != nil {
				return err
			}
		}
	} else {
		err = runDownloadWhole(ctx, &opts)
		if err != nil {
			return err
		}
	}

	if !opts.ToStdOut() {
		d.Logger().Infof(`File "%d" downloaded to "%s".`, opts.File.ID, opts.Output)
	}

	return nil
}

// download slices to `opts.Output` as one file.
func runDownloadForceUnsliced(ctx context.Context, opts *Options, logger log.Logger) error {
	logger.Infof(`Creating file "%s"`, opts.Output)
	dst, err := opts.GetOutput("")
	if err != nil {
		return err
	}
	defer func() {
		err = dst.Close()
	}()

	logger.Infof("Downloading slices")
	return downloadSliced(ctx, opts.File, func(reader io.ReadCloser, slice string, size int64, index, total int) (err error) {
		bar := progressbar.DefaultBytes(
			size,
			fmt.Sprintf(`downloading slice "%s" %d/%d`, strhelper.Truncate(slice, 20, "..."), index+1, total),
		)

		if path.Ext(slice) == ".gz" {
			reader, err = gzip.NewReader(reader)
			if err != nil {
				return err
			}
		}

		_, err = io.CopyN(io.MultiWriter(dst, bar), reader, size)
		return err
	})
}

// download slices to `opts.Output` as individual files.
func runDownloadSliced(ctx context.Context, opts *Options) error {
	if !opts.ToStdOut() {
		err := os.MkdirAll(opts.Output, 0o755) // nolint: forbidigo
		if err != nil && !os.IsExist(err) {    // nolint: forbidigo
			return errors.Errorf(`cannot create directory "%s": %w`, opts.Output, err)
		}
	}

	return downloadSliced(ctx, opts.File, func(reader io.ReadCloser, slice string, size int64, index int, total int) (err error) {
		bar := progressbar.DefaultBytes(
			size,
			fmt.Sprintf(`downloading slice "%s" %d/%d`, strhelper.Truncate(slice, 20, "..."), index+1, total),
		)

		output, err := opts.GetOutput(slice)
		if err != nil {
			return err
		}
		defer func() {
			err = output.Close()
		}()

		_, err = io.Copy(io.MultiWriter(output, bar), reader)
		return err
	})
}

// download whole file to `opts.Output`.
func runDownloadWhole(ctx context.Context, opts *Options) (err error) {
	dst, err := opts.GetOutput("")
	if err != nil {
		return err
	}
	defer func() {
		err = dst.Close()
	}()

	r, err := keboola.DownloadReader(ctx, opts.File)
	if err != nil {
		return err
	}

	attrs, err := keboola.GetFileAttributes(ctx, opts.File, "")
	if err != nil {
		return errors.Errorf("cannot get storage file attributes: %w", err)
	}
	bar := progressbar.DefaultBytes(attrs.Size, "downloading")

	_, err = io.Copy(io.MultiWriter(dst, bar), r)
	return err
}

type sliceHandler func(reader io.ReadCloser, slice string, size int64, index int, total int) error

func downloadSliced(
	ctx context.Context,
	file *keboola.FileDownloadCredentials,
	handler sliceHandler,
) error {
	slices, err := keboola.DownloadManifest(ctx, file)
	if err != nil {
		return err
	}

	for i, slice := range slices {
		r, err := keboola.DownloadSliceReader(ctx, file, slice)
		if err != nil {
			return errors.Errorf(`cannot download slice "%s": %w`, slice, err)
		}

		attrs, err := keboola.GetFileAttributes(ctx, file, slice)
		if err != nil {
			return errors.Errorf(`cannot get slice attributes "%s": %w`, slice, err)
		}

		err = handler(r, slice, attrs.Size, i, len(slices))
		if err != nil {
			return err
		}
	}

	return nil
}

// outputWriter wraps either a file or stdout, and implements `io.WriteCloser`.
//
// Close is a no-op if writing to stdout.
type outputWriter struct {
	file *os.File
}

func (o *outputWriter) Write(b []byte) (n int, err error) {
	if o.file != nil {
		return o.file.Write(b)
	} else {
		return os.Stdout.Write(b)
	}
}

func (o *outputWriter) Close() error {
	if o.file != nil {
		return o.file.Close()
	} else {
		return nil
	}
}
