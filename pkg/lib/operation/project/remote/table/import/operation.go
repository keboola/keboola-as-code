package tableimport

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

type Options struct {
	FileKey         keboola.FileKey
	TableKey        keboola.TableKey
	Columns         []string
	Delimiter       string
	Enclosure       string
	EscapedBy       string
	IncrementalLoad bool
	WithoutHeaders  bool
	PrimaryKey      []string
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.table.import")
	defer span.End(&err)

	if !checkTableExists(ctx, d, o.TableKey) {
		d.Logger().InfofCtx(ctx, `Table "%s" does not exist, creating it.`, o.TableKey.TableID)

		rb := rollback.New(d.Logger())
		err = EnsureBucketExists(ctx, d, rb, o.TableKey.BucketKey())
		if err != nil {
			return err
		}

		_, err = d.KeboolaProjectAPI().CreateTableFromFileRequest(o.TableKey, o.FileKey, getCreateOptions(&o)...).Send(ctx)
		if err != nil {
			rb.Invoke(ctx)
			return err
		}

		d.Logger().InfofCtx(ctx, `Created new table "%s" from file with id "%d".`, o.TableKey.TableID, o.FileKey.FileID)
	} else {
		job, err := d.KeboolaProjectAPI().LoadDataFromFileRequest(o.TableKey, o.FileKey, getLoadOptions(&o)...).Send(ctx)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()

		err = d.KeboolaProjectAPI().WaitForStorageJob(ctx, job)
		if err != nil {
			return err
		}
		d.Logger().InfofCtx(ctx, `Loaded data from file "%d" into table "%s".`, o.FileKey.FileID, o.TableKey.TableID)
	}

	return nil
}

func getCreateOptions(o *Options) []keboola.CreateTableOption {
	opts := make([]keboola.CreateTableOption, 0)
	opts = append(opts, keboola.WithPrimaryKey(o.PrimaryKey))
	return opts
}

func getLoadOptions(o *Options) []keboola.LoadDataOption {
	opts := make([]keboola.LoadDataOption, 0)
	if len(o.Columns) > 0 {
		opts = append(opts, keboola.WithColumnsHeaders(o.Columns))
	}
	opts = append(opts, keboola.WithDelimiter(o.Delimiter))
	opts = append(opts, keboola.WithEnclosure(o.Enclosure))
	opts = append(opts, keboola.WithEscapedBy(o.EscapedBy))
	opts = append(opts, keboola.WithIncrementalLoad(o.IncrementalLoad))
	opts = append(opts, keboola.WithoutHeader(o.WithoutHeaders))
	return opts
}

func EnsureBucketExists(ctx context.Context, d dependencies, rb rollback.Builder, bucketKey keboola.BucketKey) error {
	err := d.KeboolaProjectAPI().GetBucketRequest(bucketKey).SendOrErr(ctx)
	var apiErr *keboola.StorageError
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.buckets.notFound" {
		d.Logger().InfofCtx(ctx, `Bucket "%s" does not exist, creating it.`, bucketKey.BucketID)
		api := d.KeboolaProjectAPI()
		// Bucket doesn't exist -> create it
		bucket := &keboola.Bucket{BucketKey: bucketKey}
		if _, err := api.CreateBucketRequest(bucket).Send(ctx); err != nil {
			return err
		}
		rb.Add(func(ctx context.Context) error {
			_, err := api.DeleteBucketRequest(bucketKey).Send(ctx)
			return err
		})
	}
	return nil
}

func checkTableExists(ctx context.Context, d dependencies, tableKey keboola.TableKey) bool {
	err := d.KeboolaProjectAPI().GetTableRequest(tableKey).SendOrErr(ctx)
	var apiErr *keboola.StorageError
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.tables.notFound" {
		return false
	}
	return true
}
