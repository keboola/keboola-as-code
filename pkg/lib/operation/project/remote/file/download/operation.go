// Package download contains the implementation of the "kbc project remote file download" command.
package download

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/klauspost/pgzip"
	"github.com/schollz/progressbar/v3"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	StdoutOutput           = "-"
	GZIPFileExt            = ".gz"
	CSVFileExt             = ".csv"
	GetFileSizeParallelism = 100
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
	Stderr() io.Writer
}

type downloader struct {
	dependencies
	options Options
	bar     *progressbar.ProgressBar
	slices  []string
}

func Run(ctx context.Context, o Options, d dependencies) (returnErr error) {
	return (&downloader{options: o, dependencies: d}).Download(ctx)
}

func (d *downloader) Download(ctx context.Context) (returnErr error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.file.download")
	defer span.End(&returnErr)

	// Log start and end
	if !d.options.ToStdout() {
		defer func() {
			if returnErr == nil {
				d.Logger().Infof(ctx, `File "%d" downloaded to "%s".`, d.options.File.FileID, d.options.FormattedOutput())
			}
		}()
	}

	// Get slices
	if slices, err := d.getSlices(ctx); err == nil {
		d.slices = slices
	} else {
		return err
	}

	// Get total size
	size, err := d.totalSize(ctx)
	if err != nil {
		return err
	}

	// Create progress bar, it writes to stderr
	d.bar = progressbar.DefaultBytes(size, "Downloading")
	defer func() {
		if closeErr := d.bar.Close(); returnErr == nil && closeErr != nil {
			returnErr = closeErr
		}
	}()

	stderr := d.Stderr()
	progressbar.OptionSetWriter(stderr)(d.bar)
	progressbar.OptionOnCompletion(func() { fmt.Fprint(stderr, "\n") })

	// Download
	if d.options.ToStdout() || !d.options.AllowSliced || !d.options.File.IsSliced {
		// Download all slices into single file
		if output, err := d.openOutput(""); err != nil {
			return err
		} else if err := d.readMergedSlicesTo(ctx, output); err != nil {
			return err
		} else if err := output.Close(); err != nil {
			return err
		}
	} else {
		// Create output directory
		if err := os.MkdirAll(d.options.Output, 0o700); err != nil {
			return err
		}

		// Download all slices as separate files
		for _, slice := range d.slices {
			if output, err := d.openOutput(slice); err != nil {
				return err
			} else if err := d.readSliceTo(ctx, slice, output); err != nil {
				return err
			} else if err := output.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

type nopCloser struct {
	io.Writer
}

func (n *nopCloser) Close() error {
	return nil
}

func (d *downloader) openOutput(slice string) (io.WriteCloser, error) {
	switch {
	case d.options.ToStdout():
		return &nopCloser{d.Stdout()}, nil // stdout should not be closed
	case d.options.AllowSliced && d.options.File.IsSliced:
		return os.OpenFile(filepath.Join(d.options.Output, slice), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) // nolint:forbidigo
	default:
		return os.OpenFile(d.options.Output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) // nolint:forbidigo
	}
}

// readSliceTo to the pipe writer.
func (d *downloader) readMergedSlicesTo(ctx context.Context, writer io.Writer) error {
	for _, slice := range d.slices {
		if err := d.readSliceTo(ctx, slice, writer); err != nil {
			return err
		}
	}
	return nil
}

// readSliceTo to the pipe writer.
func (d *downloader) readSliceTo(ctx context.Context, slice string, writer io.Writer) (returnErr error) {
	var reader io.Reader

	// Create slice reader
	if sliceReader, err := keboola.DownloadSliceReader(ctx, d.options.File, slice); err == nil {
		defer func() {
			if closeErr := sliceReader.Close(); returnErr == nil && closeErr != nil {
				returnErr = closeErr
			}
		}()
		reader = sliceReader
	} else {
		return errors.Errorf(`cannot download file: %w`, err)
	}

	// Move progress bar on read
	if d.bar != nil {
		barReader := progressbar.NewReader(reader, d.bar)
		reader = &barReader
	}

	// Add decompression reader
	if strings.HasSuffix(slice, GZIPFileExt) || (slice == "" && strings.HasSuffix(d.options.File.Name, GZIPFileExt)) {
		if gzipReader, err := pgzip.NewReader(reader); err == nil {
			defer func() {
				if closeErr := gzipReader.Close(); returnErr == nil && closeErr != nil {
					returnErr = closeErr
				}
			}()
			reader = gzipReader
		} else {
			return errors.Errorf(`cannot create gzip reader: %w`, err)
		}
	}

	if strings.HasSuffix(d.options.Output, CSVFileExt) && d.options.Header.IsSet() {
		if err := d.addHeaderToCSV(writer); err != nil {
			return err
		}
	}

	// Copy all
	_, err := io.Copy(writer, reader)
	return err
}

func (d *downloader) addHeaderToCSV(writer io.Writer) error {
	w := csv.NewWriter(writer)
	defer w.Flush()

	return w.Write(d.options.Columns)
}

// getSlices from the file manifest.
func (d *downloader) getSlices(ctx context.Context) ([]string, error) {
	if d.options.File.IsSliced {
		// Sliced file
		return keboola.DownloadManifest(ctx, d.options.File)
	} else {
		// Simple file
		return []string{""}, nil
	}
}

// totalSize sums size of all slices.
func (d *downloader) totalSize(ctx context.Context) (size int64, err error) {
	atomicSize := atomic.NewInt64(0)
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(GetFileSizeParallelism)
	for _, slice := range d.slices {
		grp.Go(func() error {
			// Check context cancellation
			if err := ctx.Err(); err != nil {
				return err
			}

			// Get slice size
			if attrs, err := keboola.GetFileAttributes(ctx, d.options.File, slice); err == nil {
				atomicSize.Add(attrs.Size)
				return nil
			} else {
				return err
			}
		})
	}

	// Wait for all goroutines
	if err := grp.Wait(); err != nil {
		return 0, err
	}

	return atomicSize.Load(), nil
}
