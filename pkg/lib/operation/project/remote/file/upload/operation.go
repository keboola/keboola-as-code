// Package upload contains the implementation of the "kbc project remote file upload" command.
package upload

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/schollz/progressbar/v3"
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
	Input string
	Name  string
	Tags  []string
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.file.upload")
	defer telemetry.EndSpan(span, &err)

	var reader io.Reader
	var bar *progressbar.ProgressBar
	if o.Input == "-" {
		reader = bufio.NewReader(os.Stdin)
	} else {
		file, err := os.Open(o.Input) // nolint: forbidigo
		if err != nil {
			if os.IsNotExist(err) {
				return errors.Errorf("file %s not found", o.Input)
			}
			return errors.Errorf(`error reading file "%s": %w`, o.Input, err)
		}
		fi, err := file.Stat()
		if err != nil {
			return errors.Errorf(`error reading file "%s": %w`, o.Input, err)
		}
		bar = progressbar.DefaultBytes(fi.Size(), "uploading")
		reader = bufio.NewReader(file)
	}

	opts := make([]keboola.CreateFileOption, 0)
	if len(o.Tags) > 0 {
		opts = append(opts, keboola.WithTags(o.Tags...))
	}

	file, err := d.KeboolaProjectAPI().CreateFileResourceRequest(o.Name, opts...).Send(ctx)
	if err != nil {
		return errors.Errorf(`error creating file resource: %w`, err)
	}

	blobWriter, err := keboola.NewUploadWriter(ctx, file)
	defer func() {
		err = blobWriter.Close()
	}()
	if err != nil {
		return errors.Errorf(`error uploading file "%s": %w`, o.Input, err)
	}
	var writer io.Writer
	if bar != nil {
		writer = io.MultiWriter(blobWriter, bar)
	} else {
		writer = blobWriter
	}
	_, err = io.Copy(writer, reader)
	if err != nil {
		return errors.Errorf(`error uploading file "%s": %w`, o.Input, err)
	}
	d.Logger().Infof(`File "%s" uploaded with file id "%d".`, o.Name, file.ID)
	return nil
}
