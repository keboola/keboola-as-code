package table

import (
	"context"
	"reflect"
	"sync"

	"github.com/keboola/go-client/pkg/keboola"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	keboolaProjectAPI *keboola.API

	lock               *sync.Mutex
	singleGetBucket    *singleflight.Group
	singleCreateBucket *singleflight.Group
}

func NewManager(keboolaProjectAPI *keboola.API) *Manager {
	return &Manager{
		keboolaProjectAPI:  keboolaProjectAPI,
		lock:               &sync.Mutex{},
		singleGetBucket:    &singleflight.Group{},
		singleCreateBucket: &singleflight.Group{},
	}
}

func (m *Manager) ImportFile(ctx context.Context, file model.File) (err error) {
	r := m.keboolaProjectAPI.
		LoadDataFromFileRequest(file.Mapping.TableID, file.StorageResource.ID, keboola.WithIncrementalLoad(file.Mapping.Incremental), keboola.WithoutHeader(true)).
		WithOnSuccess(func(ctx context.Context, job *keboola.StorageJob) error {
			return m.keboolaProjectAPI.WaitForStorageJob(ctx, job)
		})
	return r.SendOrErr(ctx)
}

func (m *Manager) EnsureTablesExist(ctx context.Context, rb rollback.Builder, receiver *model.Receiver) (err error) {
	rb = rb.AddParallel()
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	for i := range receiver.Exports {
		export := &receiver.Exports[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.EnsureTableExists(ctx, rb, export); err != nil {
				errs.Append(err)
			}
		}()
	}
	wg.Wait()
	return errs.ErrorOrNil()
}

func (m *Manager) EnsureTableExists(ctx context.Context, rb rollback.Builder, export *model.Export) error {
	tableID := export.Mapping.TableID
	columns := export.Mapping.Columns.Names()
	primaryKey := export.Mapping.Columns.PrimaryKey()

	table, err := m.keboolaProjectAPI.GetTableRequest(tableID).Send(ctx)
	var apiErr *keboola.StorageError
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.tables.notFound" {
		// Table doesn't exist -> create it
		if req, err := m.keboolaProjectAPI.CreateTableDeprecatedSyncRequest(tableID, columns, keboola.WithPrimaryKey(primaryKey)); err != nil {
			return err
		} else if table, err = req.Send(ctx); err != nil {
			return err
		}
		rb.Add(func(ctx context.Context) error {
			_, err := m.keboolaProjectAPI.DeleteTableRequest(tableID).Send(ctx)
			return err
		})
	} else if err != nil {
		// Other error
		return err
	}

	// Check columns
	if !reflect.DeepEqual(columns, table.Columns) {
		return serviceError.NewBadRequestError(errors.Errorf(
			`columns of the table "%s" do not match expected %s, found %s`,
			table.ID.String(), json.MustEncodeString(columns, false), json.MustEncodeString(table.Columns, false),
		))
	}
	// Check primary key
	if !reflect.DeepEqual(primaryKey, table.PrimaryKey) {
		return serviceError.NewBadRequestError(errors.Errorf(
			`primary key of the table "%s" does not match expected %s, found %s`,
			table.ID.String(), json.MustEncodeString(primaryKey, false), json.MustEncodeString(table.PrimaryKey, false),
		))
	}

	return nil
}

func (m *Manager) EnsureBucketsExist(ctx context.Context, rb rollback.Builder, receiver *model.Receiver) (err error) {
	rb = rb.AddParallel()
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	for i := range receiver.Exports {
		export := &receiver.Exports[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.EnsureBucketExists(ctx, rb, export.Mapping.TableID.BucketID); err != nil {
				errs.Append(err)
			}
		}()
	}
	wg.Wait()
	return errs.ErrorOrNil()
}

func (m *Manager) EnsureBucketExists(ctx context.Context, rb rollback.Builder, bucketID keboola.BucketID) error {
	// Check if bucket exists
	_, err := m.getBucket(ctx, bucketID)
	var apiErr *keboola.StorageError
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.buckets.notFound" {
		// Bucket doesn't exist -> create it
		if _, err := m.createBucket(ctx, rb, bucketID); err != nil {
			return err
		}
	} else if err != nil {
		// Other error
		return err
	}
	return nil
}

func (m *Manager) getBucket(ctx context.Context, bucketID keboola.BucketID) (*keboola.Bucket, error) {
	bucket, err, _ := m.singleGetBucket.Do(bucketID.String(), func() (any, error) {
		return m.keboolaProjectAPI.GetBucketRequest(bucketID).Send(ctx)
	})
	return bucket.(*keboola.Bucket), err
}

func (m *Manager) createBucket(ctx context.Context, rb rollback.Builder, bucketID keboola.BucketID) (*keboola.Bucket, error) {
	bucket, err, _ := m.singleCreateBucket.Do(bucketID.String(), func() (any, error) {
		bucket := &keboola.Bucket{ID: bucketID}
		if _, err := m.keboolaProjectAPI.CreateBucketRequest(bucket).Send(ctx); err != nil {
			return nil, err
		}
		rb.Add(func(ctx context.Context) error {
			_, err := m.keboolaProjectAPI.DeleteBucketRequest(bucketID).Send(ctx)
			return err
		})
		return bucket, nil
	})
	return bucket.(*keboola.Bucket), err
}
