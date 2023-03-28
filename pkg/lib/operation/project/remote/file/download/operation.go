// Package download contains the implementation of the "kbc project remote file download" command.
package download

import (
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
	File   *keboola.FileDownloadCredentials
	Output string
	// `ForceUnsliced == true`` should be mutually exclusive with `Output == "-"`
	ForceUnsliced bool
}

func (o *Options) ToStdOut() bool {
	return o.Output == "-"
}

func Run(ctx context.Context, opts Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.file.download")
	defer telemetry.EndSpan(span, &err)

	if opts.ForceUnsliced && opts.ToStdOut() {
		panic("invalid options: ForceUnsliced == true && Output == \"-\"")
	}

	if opts.File.IsSliced {
		if opts.ForceUnsliced {
			err = runForceUnsliced(ctx, &opts, d.Logger())
			if err != nil {
				return err
			}
		} else {
			err = runSliced(ctx, &opts)
			if err != nil {
				return err
			}
		}
	} else {
		err = runWhole(ctx, &opts)
		if err != nil {
			return err
		}
	}

	if !opts.ToStdOut() {
		d.Logger().Infof(`File "%d" downloaded to "%s".`, opts.File.ID, opts.Output)
	}

	return nil
}

// download slices to temp directory, then copy all slices to final file.
func runForceUnsliced(ctx context.Context, opts *Options, logger log.Logger) error {
	tempDir := opts.Output + ".temp"
	logger.Infof("Creating temp directory %s", tempDir)
	err := os.MkdirAll(tempDir, 0o755)  // nolint: forbidigo
	if err != nil && !os.IsExist(err) { // nolint: forbidigo
		return errors.Errorf("cannot create temporary directory %s: %w", tempDir, err)
	}
	defer func() {
		logger.Infof("Deleting temp directory %s", tempDir)
		os.RemoveAll(tempDir) // nolint: forbidigo
	}()

	logger.Infof("Downloading slices")
	slices, err := downloadSlicesTo(ctx, opts.File, tempDir)
	if err != nil {
		return err
	}

	logger.Infof("Creating file %s", opts.Output)
	file, err := os.Create(opts.Output) // nolint: forbidigo
	if err != nil {
		return errors.Errorf("cannot create file %s: %w", opts.Output, err)
	}

	logger.Infof("Copying slices from %s to %s", tempDir, opts.Output)
	offset := int64(0)
	for _, slice := range slices {
		data, err := os.ReadFile(slice) // nolint: forbidigo
		if err != nil {
			return err
		}

		_, err = file.WriteAt(data, offset)
		if err != nil {
			return errors.Errorf("failed to write data to file %s: %w", opts.Output, err)
		}
		offset += int64(len(data))
	}

	return nil
}

// download slices to `opts.Output`.
func runSliced(ctx context.Context, opts *Options) error {
	if !opts.ToStdOut() {
		err := os.MkdirAll(opts.Output, 0o755) // nolint: forbidigo
		if err != nil && !os.IsExist(err) {    // nolint: forbidigo
			return errors.Errorf("cannot create directory %s: %w", opts.Output, err)
		}
	}

	_, err := downloadSlicesTo(ctx, opts.File, opts.Output)
	return err
}

// download file to `opts.Output`.
func runWhole(ctx context.Context, opts *Options) error {
	r, err := keboola.DownloadReader(ctx, opts.File)
	if err != nil {
		return err
	}

	attrs, err := keboola.GetFileAttributes(ctx, opts.File, "")
	if err != nil {
		return errors.Errorf("cannot get file attributes: %w", err)
	}
	bar := progressbar.DefaultBytes(attrs.Size, "downloading")

	return download(r, opts.Output, bar)
}

func downloadSlicesTo(ctx context.Context, file *keboola.FileDownloadCredentials, outputPath string) ([]string, error) {
	var slicePaths []string

	slices, err := keboola.DownloadManifest(ctx, file)
	if err != nil {
		return nil, err
	}

	for i, slice := range slices {
		r, err := keboola.DownloadSliceReader(ctx, file, slice)
		if err != nil {
			return nil, errors.Errorf("cannot download slice %s: %w", slice, err)
		}

		attrs, err := keboola.GetFileAttributes(ctx, file, slice)
		if err != nil {
			return nil, errors.Errorf("cannot get slice attributes %s: %w", slice, err)
		}
		bar := progressbar.DefaultBytes(attrs.Size, fmt.Sprintf(`downloading slice "%s" %d/%d`, strhelper.Truncate(slice, 20, "..."), i+1, len(slices)))

		sliceOutputPath := "-"
		if outputPath != "-" {
			sliceOutputPath = path.Join(outputPath, slice)
		}

		err = download(r, sliceOutputPath, bar)
		if err != nil {
			return nil, errors.Errorf("cannot download slice %s: %w", slice, err)
		}
		slicePaths = append(slicePaths, sliceOutputPath)
	}

	return slicePaths, nil
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
