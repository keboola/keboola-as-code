// Package download contains the implementation of the "kbc project remote file download" command.
package download

import (
	"context"
	"io"
	"os"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

func Run(ctx context.Context, opts Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.file.download")
	defer telemetry.EndSpan(span, &err)

	if opts.File.IsSliced {
		if opts.Output != "-" {
			err = os.Mkdir(opts.Output, 0o755)  // nolint: forbidigo
			if err != nil && !os.IsExist(err) { // nolint: forbidigo
				return errors.Errorf("cannot create directory %s: %w", opts.Output, err)
			}
		}

		slices, err := keboola.DownloadManifest(ctx, opts.File)
		for _, slice := range slices {
			r, err := keboola.DownloadSliceReader(ctx, opts.File, slice)
			if err != nil {
				return errors.Errorf("cannot download slice %s: %w", slice, err)
			}
			output := "-"
			if opts.Output != "-" {
				output = opts.Output + "/" + slice
			}
			err = download(r, output)
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
		err = download(r, opts.Output)
		if err != nil {
			return err
		}
	}
	if opts.Output != "-" {
		d.Logger().Infof(`File "%d" downloaded to "%s".`, opts.File.ID, opts.Output)
	}

	return nil
}

func download(r io.ReadCloser, output string) (err error) {
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

	_, err = io.Copy(dst, r)
	return err
}
