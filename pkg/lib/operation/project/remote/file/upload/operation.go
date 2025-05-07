// Package upload contains the implementation of the "kbc project remote file upload" command.
package upload

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/schollz/progressbar/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

type Options struct {
	BranchKey keboola.BranchKey
	Input     string
	Name      string
	Tags      []string
}

func Run(ctx context.Context, o Options, d dependencies) (f *keboola.FileUploadCredentials, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.file.upload")
	defer span.End(&err)

	opts := make([]keboola.CreateFileOption, 0)
	if len(o.Tags) > 0 {
		opts = append(opts, keboola.WithTags(o.Tags...))
	}

	// Check files existence
	var fr *os.File
	var fi os.FileInfo
	if o.Input != "-" {
		fr, err = os.Open(o.Input) // nolint: forbidigo
		if err != nil {
			if os.IsNotExist(err) {
				return nil, errors.Errorf("file %s not found", o.Input)
			}
			return nil, errors.Errorf(`error reading file "%s": %w`, o.Input, err)
		}
		fi, err = fr.Stat()
		if err != nil {
			return nil, errors.Errorf(`error reading file "%s": %w`, o.Input, err)
		}

		if fi.IsDir() {
			files, err := os.ReadDir(o.Input)
			if err != nil {
				return nil, errors.Errorf(`error reading files in folder "%s": %w`, o.Input, err)
			}
			for _, f := range files {
				_, err := f.Info()
				if err != nil {
					return nil, errors.Errorf(`error reading file "%s" in folder "%s": %w`, f.Name(), o.Input, err)
				}
			}
			opts = append(opts, keboola.WithIsSliced(true))
		}
	}

	file, err := d.KeboolaProjectAPI().CreateFileResourceRequest(o.BranchKey.ID, o.Name, opts...).Send(ctx)
	if err != nil {
		return nil, errors.Errorf(`error creating file resource: %w`, err)
	}

	// Upload from stdin
	if o.Input == "-" {
		err = upload(ctx, file, bufio.NewReader(os.Stdin), nil)
		if err != nil {
			return nil, errors.Errorf(`error uploading file from stdin: %w`, err)
		}
		d.Logger().Infof(ctx, `File "%s" uploaded with file id "%d".`, o.Name, file.FileID)
		return file, nil
	}

	// Upload single file
	if !fi.IsDir() {
		bar := progressbar.DefaultBytes(fi.Size(), "uploading")
		reader := bufio.NewReader(fr)
		err = upload(ctx, file, reader, bar)
		if err != nil {
			return nil, errors.Errorf(`error uploading file "%s": %w`, o.Input, err)
		}
		d.Logger().Infof(ctx, `File "%s" uploaded with file id "%d".`, o.Name, file.FileID)
		return file, nil
	}

	// Upload sliced file from folder
	files, err := os.ReadDir(o.Input)
	if err != nil {
		return nil, errors.Errorf(`error reading files in folder "%s": %w`, o.Input, err)
	}
	for i, f := range files {
		fr, err = os.Open(fmt.Sprintf("%s/%s", o.Input, f.Name())) // nolint: forbidigo
		if err != nil {
			return nil, errors.Errorf(`error reading file "%s" in folder "%s": %w`, f.Name(), o.Input, err)
		}
		fInfo, err := f.Info()
		if err != nil {
			return nil, errors.Errorf(`error reading file "%s" in folder "%s": %w`, f.Name(), o.Input, err)
		}
		bar := progressbar.DefaultBytes(fInfo.Size(), fmt.Sprintf(`uploading slice "%s" %d/%d`, strhelper.Truncate(f.Name(), 20, "..."), i+1, len(files)))
		err = upload(ctx, file, fr, bar)
		if err != nil {
			return nil, errors.Errorf(`error uploading file "%s": %w`, o.Input, err)
		}
	}

	d.Logger().Infof(ctx, `File "%s" uploaded with file id "%d".`, o.Name, file.FileID)
	return file, nil
}

func upload(ctx context.Context, file *keboola.FileUploadCredentials, reader io.Reader, bar *progressbar.ProgressBar) (err error) {
	blobWriter, err := keboola.NewUploadWriter(ctx, file)
	defer func() {
		err = blobWriter.Close()
	}()
	if err != nil {
		return err
	}
	var writer io.Writer
	if bar != nil {
		writer = io.MultiWriter(blobWriter, bar)
	} else {
		writer = blobWriter
	}
	_, err = io.Copy(writer, reader)
	if err != nil {
		return err
	}
	return nil
}
