package table

import (
	"context"
	"reflect"
	"sync"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	client client.Sender // Storage API client

	lock               *sync.Mutex
	singleGetBucket    *singleflight.Group
	singleCreateBucket *singleflight.Group
}

func NewManager(client client.Sender) *Manager {
	return &Manager{
		client:             client,
		lock:               &sync.Mutex{},
		singleGetBucket:    &singleflight.Group{},
		singleCreateBucket: &singleflight.Group{},
	}
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

	table, err := storageapi.GetTableRequest(tableID).Send(ctx, m.client)
	var apiErr *storageapi.Error
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.tables.notFound" {
		var opts []storageapi.CreateTableOption
		if len(primaryKey) > 0 {
			opts = append(opts, storageapi.WithPrimaryKey(primaryKey))
		}
		// Table doesn't exist -> create it
		if req, err := storageapi.CreateTableDeprecatedSyncRequest(tableID, columns, opts...); err != nil {
			return err
		} else if table, err = req.Send(ctx, m.client); err != nil {
			return err
		}
		rb.Add(func(ctx context.Context) error {
			_, err := storageapi.DeleteTableRequest(tableID).Send(ctx, m.client)
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

func (m *Manager) EnsureBucketExists(ctx context.Context, rb rollback.Builder, bucketID storageapi.BucketID) error {
	// Check if bucket exists
	_, err := m.getBucket(ctx, bucketID)
	var apiErr *storageapi.Error
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

func (m *Manager) getBucket(ctx context.Context, bucketID storageapi.BucketID) (*storageapi.Bucket, error) {
	bucket, err, _ := m.singleGetBucket.Do(bucketID.String(), func() (any, error) {
		return storageapi.GetBucketRequest(bucketID).Send(ctx, m.client)
	})
	return bucket.(*storageapi.Bucket), err
}

func (m *Manager) createBucket(ctx context.Context, rb rollback.Builder, bucketID storageapi.BucketID) (*storageapi.Bucket, error) {
	bucket, err, _ := m.singleCreateBucket.Do(bucketID.String(), func() (any, error) {
		bucket := &storageapi.Bucket{ID: bucketID}
		if _, err := storageapi.CreateBucketRequest(bucket).Send(ctx, m.client); err != nil {
			return nil, err
		}
		rb.Add(func(ctx context.Context) error {
			_, err := storageapi.DeleteBucketRequest(bucketID).Send(ctx, m.client)
			return err
		})
		return bucket, nil
	})
	return bucket.(*storageapi.Bucket), err
}
