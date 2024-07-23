package bridge

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (b *Bridge) ensureBucketExistsBlocking(ctx context.Context, api *keboola.AuthorizedAPI, tableKey keboola.TableKey) error {
	// Check if the bucket exists
	_, err := b.getBucket(ctx, api, tableKey.BucketKey())

	// Try to create bucket, if not exists
	var apiErr *keboola.StorageError
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.buckets.notFound" {
		_, err = b.createBucket(ctx, api, tableKey.BucketKey())
	}

	return err
}

func (b *Bridge) getBucket(ctx context.Context, api *keboola.AuthorizedAPI, bucketKey keboola.BucketKey) (*keboola.Bucket, error) {
	ctx = ctxattr.ContextWith(ctx, attribute.String("bucket.key", bucketKey.String()))
	bucket, err, _ := b.getBucketOnce.Do(bucketKey.String(), func() (any, error) {
		return api.GetBucketRequest(bucketKey).Send(ctx)
	})

	if err != nil {
		err = errors.Errorf(`cannot get bucket: %w`, err)

		var apiErr *keboola.StorageError
		if !errors.As(err, &apiErr) || apiErr.ErrCode != "storage.buckets.notFound" {
			b.logger.Warn(ctx, err.Error())
		}

		return nil, err
	}

	b.logger.Info(ctx, "bucket exists")
	return bucket.(*keboola.Bucket), nil
}

func (b *Bridge) createBucket(ctx context.Context, api *keboola.AuthorizedAPI, bucketKey keboola.BucketKey) (*keboola.Bucket, error) {
	ctx = ctxattr.ContextWith(ctx, attribute.String("bucket.key", bucketKey.String()))

	bucket, err, _ := b.createBucketOnce.Do(bucketKey.String(), func() (any, error) {
		// Create bucket
		b.logger.Info(ctx, "creating bucket")
		bucket := &keboola.Bucket{BucketKey: bucketKey}
		if _, err := api.CreateBucketRequest(bucket).Send(ctx); err != nil {
			return nil, err
		}

		// Register rollback
		rollback.FromContext(ctx).Add(func(ctx context.Context) error {
			b.logger.Info(ctx, "rollback: deleting bucket")
			// No "force" option, so bucket will be deleted only if it is empty
			return api.DeleteBucketRequest(bucketKey).SendOrErr(ctx)
		})

		return bucket, nil
	})

	if err != nil {
		err = errors.Errorf(`cannot create bucket "%s": %w`, bucketKey.BucketID, err)
		b.logger.Warn(ctx, err.Error())
		return nil, err
	}

	b.logger.Info(ctx, "created bucket")
	return bucket.(*keboola.Bucket), nil
}
