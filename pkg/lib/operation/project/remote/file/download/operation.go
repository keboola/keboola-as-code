// Package download contains the implementation of the "kbc project remote file download" command.
package download

import (
	"context"
	"fmt"
	"io"
	"os"

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
	File   *keboola.FileDownloadCredentials
	Output string
}

func (o *Options) ToStdOut() bool {
	return o.Output == "-"
}

func Run(ctx context.Context, opts Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.file.download")
	defer telemetry.EndSpan(span, &err)

	if opts.File.IsSliced {
		if !opts.ToStdOut() {
			err = os.MkdirAll(opts.Output, 0o755) // nolint: forbidigo
			if err != nil && !os.IsExist(err) {   // nolint: forbidigo
				return errors.Errorf("cannot create directory %s: %w", opts.Output, err)
			}
		}

		slices, err := keboola.DownloadManifest(ctx, opts.File)
		for i, slice := range slices {
			r, err := keboola.DownloadSliceReader(ctx, opts.File, slice)
			if err != nil {
				return errors.Errorf("cannot download slice %s: %w", slice, err)
			}
			output := "-"
			if opts.Output != "-" {
				output = opts.Output + "/" + slice
			}

			attrs, err := keboola.GetFileAttributes(ctx, opts.File, slice)
			if err != nil {
				return errors.Errorf("cannot get slice attributes %s: %w", slice, err)
			}
			bar := progressbar.DefaultBytes(attrs.Size, fmt.Sprintf(`downloading slice "%s" %d/%d`, strhelper.Truncate(slice, 20, "..."), i+1, len(slices)))

			err = download(r, output, bar)
			if err != nil {
				return errors.Errorf("cannot download slice %s: %w", slice, err)
			}
		}
		if err != nil {
			return err
		}
	} else {
		r, err := keboola.DownloadReader(ctx, opts.File)
		if err != nil {
			return err
		}

		attrs, err := keboola.GetFileAttributes(ctx, opts.File, "")
		if err != nil {
			return errors.Errorf("cannot get file attributes: %w", err)
		}
		bar := progressbar.DefaultBytes(attrs.Size, "downloading")

		err = download(r, opts.Output, bar)
		if err != nil {
			return err
		}
	}
	if !opts.ToStdOut() {
		d.Logger().Infof(`File "%d" downloaded to "%s".`, opts.File.ID, opts.Output)
	}

	return nil
}

func download(r io.ReadCloser, output string, bar *progressbar.ProgressBar) (err error) {
	defer func() {
		err = r.Close()
	}()

	var dst io.WriteCloser
	if output == "-" {
		dst = os.Stdout // nolint: forbidigo
	} else {
		dst, err = os.Create(output) // nolint: forbidigo
		if err != nil {
			return err
		}
		defer func() {
			err = dst.Close()
		}()
	}

	_, err = io.Copy(io.MultiWriter(dst, bar), r)
	return err
}
