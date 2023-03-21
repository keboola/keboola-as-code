package upload

import (
	"bufio"
	"context"
	"io"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/schollz/progressbar/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Fs() filesystem.Fs
	Tracer() trace.Tracer
}

type Options struct {
	TableID         keboola.TableID
	FilePath        string
	FileName        string
	IncrementalLoad bool
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.table.upload")
	defer telemetry.EndSpan(span, &err)

	d.Logger().Infof(`Checking for bucket "%s".`, o.TableID.BucketID)
	err = ensureBucketExists(ctx, d, o.TableID.BucketID)
	if err != nil {
		return err
	}

	d.Logger().Infof(`Uploading file "%s".`, o.FilePath)
	file, err := uploadFile(ctx, &o, d)
	if err != nil {
		return err
	}
	d.Logger().Infof(`Loading file "%s" into table "%s".`, o.FileName, o.TableID)

	if !checkTableExists(ctx, d, o.TableID) {
		err := d.KeboolaProjectAPI().CreateTableFromFileRequest(o.TableID, file.ID).SendOrErr(ctx)
		if err != nil {
			return err
		}
	} else {
		job, err := d.KeboolaProjectAPI().LoadDataFromFileRequest(
			o.TableID,
			file.ID,
			keboola.WithIncrementalLoad(o.IncrementalLoad),
		).Send(ctx)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()

		err = d.KeboolaProjectAPI().WaitForStorageJob(ctx, job)
		if err != nil {
			return err
		}
	}

	d.Logger().Infof(`Uploaded "%s" to table "%s".`, o.FilePath, o.TableID)

	return nil
}

func ensureBucketExists(ctx context.Context, d dependencies, id keboola.BucketID) error {
	err := d.KeboolaProjectAPI().GetBucketRequest(id).SendOrErr(ctx)
	var apiErr *keboola.StorageError
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.buckets.notFound" {
		d.Logger().Infof(`Bucket "%s" does not exist, creating it.`, id)
		// Bucket doesn't exist -> create it
		bucket := &keboola.Bucket{ID: id}
		if _, err := d.KeboolaProjectAPI().CreateBucketRequest(bucket).Send(ctx); err != nil {
			return err
		}
	}
	d.Logger().Infof(`Bucket "%s" exists.`, id)
	return nil
}

func checkTableExists(ctx context.Context, d dependencies, id keboola.TableID) bool {
	err := d.KeboolaProjectAPI().GetTableRequest(id).SendOrErr(ctx)
	var apiErr *keboola.StorageError
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.tables.notFound" {
		// Table doesn't exist -> create it
		return false
	}
	return true
}

func uploadFile(ctx context.Context, o *Options, d dependencies) (*keboola.File, error) {
	var reader io.Reader
	var bar *progressbar.ProgressBar
	{
		file, err := d.Fs().Open(o.FilePath)
		if err != nil {
			return nil, errors.Errorf(`error reading file "%s": %w`, o.FilePath, err)
		}
		reader = bufio.NewReader(file)

		fi, err := file.Stat()
		if err != nil {
			return nil, errors.Errorf(`error reading file "%s": %w`, o.FilePath, err)
		}
		bar = progressbar.DefaultBytes(fi.Size(), "uploading")
	}

	file, err := d.KeboolaProjectAPI().CreateFileResourceRequest(o.FileName).Send(ctx)
	if err != nil {
		return nil, errors.Errorf(`error creating file resource: %w`, err)
	}

	blobWriter, err := keboola.NewUploadWriter(ctx, file)
	defer func() {
		err = blobWriter.Close()
	}()
	if err != nil {
		return nil, errors.Errorf(`error uploading file "%s": %w`, o.FilePath, err)
	}
	var writer io.Writer
	if bar != nil {
		writer = io.MultiWriter(blobWriter, bar)
	} else {
		writer = blobWriter
	}
	_, err = io.Copy(writer, reader)
	if err != nil {
		return nil, errors.Errorf(`error uploading file "%s": %w`, o.FilePath, err)
	}
	d.Logger().Infof(`File "%s" uploaded with file id "%d".`, o.FileName, file.ID)

	return &file.File, nil
}
