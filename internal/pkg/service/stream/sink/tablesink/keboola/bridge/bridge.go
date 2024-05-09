package bridge

//
// import (
//	"context"
//	"fmt"
//	"reflect"
//	"sync"
//	"time"
//
//	"github.com/keboola/go-client/pkg/keboola"
//	"golang.org/x/sync/errgroup"
//	"golang.org/x/sync/singleflight"
//
//	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
//	"github.com/keboola/keboola-as-code/internal/pkg/log"
//	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/hook"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
//	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
//	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
//)
//
// const (
//	parallelOperationsLimit = 50
//)
//
// type dependencies interface {
//	Logger() log.Logger
//	HookRegistry() *hook.Registry
//	StorageRepository() *storageRepo.Repository
//}
//
// type apiProvider func(ctx context.Context) *keboola.AuthorizedAPI
//
// type bridge struct {
//	logger      log.Logger
//	storage     *storageRepo.Repository
//	apiProvider apiProvider
//}
//
// type tableSinkPlugin struct {
//	*bridge
//
//	grp *errgroup.Group
//	rb  rollback.Builder
//	now time.Time
//	api *keboola.AuthorizedAPI
//
//	getBucketOnce    *singleflight.Group
//	createBucketOnce *singleflight.Group
//
//	lock       *sync.Mutex
//	txn        *op.TxnOp[op.NoResult]
//	finalizers []func(context.Context) error
//}
//
// func RegisterTableSinkPlugin(d dependencies, apiProvider apiProvider) {
//	b := newBridge(d, apiProvider)
//	d.HookRegistry().OnSinkSave(b.OnSinkSave)
//}
//
// func newBridge(d dependencies, apiProvider apiProvider) *bridge {
//	return &bridge{
//		logger:      d.Logger(),
//		storage:     d.StorageRepository(),
//		apiProvider: apiProvider,
//	}
//}
//
// func (b *bridge) OnSinkSave(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, sinks *[]definition.Sink, atomicOp *op.AtomicOpCore) {
//	// Load existing tokens
//	var tokens []model.Token
//	switch k := parentKey.(type) {
//	case key.SinkKey:
//		// Get
//		atomicOp.ReadOp(b.storage.Token().GetKV(k).WithOnResult(func(kv *op.KeyValueT[model.Token]) {
//			if kv != nil {
//				tokens = []model.Token{kv.Value}
//			}
//		}))
//	default:
//		// List
//		atomicOp.ReadOp(b.storage.Token().List(parentKey).WithAllTo(&tokens))
//	}
//
//	// Convert tokens slice to a map
//	var tokenMap map[key.SinkKey]*model.Token
//	atomicOp.OnWrite(func(ctx context.Context) {
//		tokenMap = make(map[key.SinkKey]*model.Token)
//		for _, token := range tokens {
//			token := token
//			tokenMap[token.SinkKey] = &token
//		}
//	})
//
//	// Call the plugin on each sink
//	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
//		ctx, plugin := b.newPlugin(ctx, rb, now)
//
//		// Check buckets, tables and tokens
//		for _, sink := range *sinks {
//			if sink.Type != definition.SinkTypeTable || sink.Table.Type != definition.TableTypeKeboola {
//				continue
//			}
//
//			if active := !sink.Deleted && !sink.Disabled; active {
//				if active {
//					plugin.OnTableSinkActivation(ctx, sink, tokenMap[sink.SinkKey])
//				} else {
//					plugin.OnTableSinkDeactivation(ctx, sink, tokenMap[sink.SinkKey])
//				}
//			}
//		}
//
//		// Create external resources
//		if err := plugin.grp.Wait(); err != nil {
//			return nil, err
//		}
//
//		return plugin.txn, nil
//	})
//}
//
// func (b *bridge) newPlugin(ctx context.Context, rb rollback.Builder, now time.Time) (context.Context, *tableSinkPlugin) {
//	// Create parallel error group
//	grp, ctx := errgroup.WithContext(ctx)
//	grp.SetLimit(parallelOperationsLimit)
//
//	c := &tableSinkPlugin{
//		bridge:           b,
//		grp:              grp,
//		rb:               rb,
//		now:              now,
//		api:              b.apiProvider(ctx),
//		getBucketOnce:    &singleflight.Group{},
//		createBucketOnce: &singleflight.Group{},
//		lock:             &sync.Mutex{},
//	}
//
//	// Create transaction for database operation.
//	// If the transaction is successful, execute finalizers.
//	c.txn = op.Txn(nil).OnSucceeded(func(r *op.TxnResult[op.NoResult]) {
//		errs := errors.NewMultiError()
//		for _, fn := range c.finalizers {
//			if err := fn(ctx); err != nil {
//				errs.Append(err)
//			}
//		}
//		if err := errs.ErrorOrNil(); err != nil {
//			err = errors.PrefixError(err, "finalization error")
//			c.logger.Error(ctx, err.Error())
//		}
//	})
//
//	return ctx, c
//}
//
// func (p *tableSinkPlugin) OnTableSinkActivation(ctx context.Context, sink definition.Sink, token *model.Token) {
//	rb := p.rb.AddParallel()
//	p.grp.Go(func() error {
//		// Ensure bucket exists
//		bucketKey := keboola.BucketKey{BranchID: sink.BranchID, BucketID: sink.Table.Keboola.TableID.BucketID}
//		if err := p.ensureBucketExists(ctx, rb, bucketKey); err != nil {
//			return err
//		}
//
//		// Ensure table exists
//		if err := p.ensureTableExists(ctx, rb, sink); err != nil {
//			return err
//		}
//
//		// Verify token, if any
//		if token != nil {
//			if ok, err := p.verifyToken(ctx, token); err != nil {
//				return err
//			} else if !ok {
//				token = nil // create a new token
//			}
//		}
//
//		// Create a new token
//		if token == nil {
//			if err := p.createToken(ctx, rb, sink.SinkKey, bucketKey); err != nil {
//				return err
//			}
//		}
//
//		return nil
//	})
//}
//
// func (p *tableSinkPlugin) OnTableSinkDeactivation(ctx context.Context, sink definition.Sink, token *model.Token) {
//	// Cleanup: delete token from the DB and finally from the Storage API.
//	if token != nil {
//		p.MergeOp(p.storage.Token().Delete(sink.SinkKey))
//		p.AddFinalizer(func(ctx context.Context) error {
//			return p.api.DeleteTokenRequest(token.Token.ID).SendOrErr(ctx)
//		})
//	}
//}
//
//// MergeOp adds a database operation to the transaction.
// func (p *tableSinkPlugin) MergeOp(operation op.Op) {
//	p.lock.Lock()
//	defer p.lock.Unlock()
//	p.txn.Merge(operation)
//}
//
//// AddFinalizer adds a finalizer function, it is called after the transaction, if succeeded.
// func (p *tableSinkPlugin) AddFinalizer(fn func(context.Context) error) {
//	p.lock.Lock()
//	defer p.lock.Unlock()
//	p.finalizers = append(p.finalizers, fn)
//}
//
// func (p *tableSinkPlugin) ensureBucketExists(ctx context.Context, rb rollback.Builder, bucketKey keboola.BucketKey) error {
//	// Check if the bucket exists
//	if _, err := p.getBucket(ctx, bucketKey); err != nil {
//		var apiErr *keboola.StorageError
//		if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.buckets.notFound" {
//			// Create the bucket
//			if _, err := p.createBucket(ctx, rb, bucketKey); err != nil {
//				return err
//			}
//		} else {
//			// Other error
//			return err
//		}
//	}
//
//	return nil
//}
//
//// getBucket once.
//func (p *tableSinkPlugin) getBucket(ctx context.Context, bucketKey keboola.BucketKey) (*keboola.Bucket, error) {
//	bucket, err, _ := p.getBucketOnce.Do(bucketKey.String(), func() (any, error) {
//		return p.api.GetBucketRequest(bucketKey).Send(ctx)
//	})
//	return bucket.(*keboola.Bucket), err
//}
//
//// createBucket once.
//func (p *tableSinkPlugin) createBucket(ctx context.Context, rb rollback.Builder, bucketKey keboola.BucketKey) (*keboola.Bucket, error) {
//	bucket, err, _ := p.createBucketOnce.Do(bucketKey.String(), func() (any, error) {
//		// Create bucket
//		bucket := &keboola.Bucket{BucketKey: bucketKey}
//		if _, err := p.api.CreateBucketRequest(bucket).Send(ctx); err != nil {
//			return nil, err
//		}
//
//		// Register rollback
//		rb.Add(func(ctx context.Context) error {
//			_, err := p.api.DeleteBucketRequest(bucketKey).Send(ctx)
//			return err
//		})
//
//		return bucket, nil
//	})
//
//	if err != nil {
//		return nil, err
//	}
//
//	return bucket.(*keboola.Bucket), nil
//}
//
//func (p *tableSinkPlugin) ensureTableExists(ctx context.Context, rb rollback.Builder, sink definition.Sink) error {
//	tableKey := keboola.TableKey{BranchID: sink.BranchID, TableID: sink.Table.Keboola.TableID}
//	columnsNames := sink.Table.Mapping.Columns.Names()
//	primaryKey := sink.Table.Mapping.Columns.PrimaryKey()
//
//	var columns []keboola.Column
//	for _, name := range columnsNames {
//		columns = append(columns, keboola.Column{
//			Name: name,
//		})
//	}
//
//	// Get table
//	table, err := p.api.GetTableRequest(tableKey).Send(ctx)
//	if err != nil {
//		var apiErr *keboola.StorageError
//		if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.tables.notFound" {
//			// Create table
//			table, err = p.api.CreateTableDefinitionRequest(tableKey, keboola.TableDefinition{
//				PrimaryKeyNames: primaryKey,
//				Columns:         columns,
//			}).Send(ctx)
//			if err != nil {
//				return err
//			}
//
//			// Register rollback
//			rb.Add(func(ctx context.Context) error {
//				_, err := p.api.DeleteTableRequest(tableKey).Send(ctx)
//				return err
//			})
//		} else {
//			// Other error
//			return err
//		}
//	}
//
//	// Check columns
//	if !reflect.DeepEqual(columnsNames, table.Columns) {
//		return serviceError.NewBadRequestError(errors.Errorf(
//			`columns of the table "%s" do not match expected %s, found %s`,
//			table.TableID.String(), json.MustEncodeString(columnsNames, false), json.MustEncodeString(table.Columns, false),
//		))
//	}
//	// Check primary key
//	if !reflect.DeepEqual(primaryKey, table.PrimaryKey) {
//		return serviceError.NewBadRequestError(errors.Errorf(
//			`primary key of the table "%s" does not match expected %s, found %s`,
//			table.TableID.String(), json.MustEncodeString(primaryKey, false), json.MustEncodeString(table.PrimaryKey, false),
//		))
//	}
//
//	return nil
//}
//
//func (p *tableSinkPlugin) verifyToken(ctx context.Context, token *model.Token) (ok bool, err error) {
//	err = p.api.VerifyTokenRequest(token.TokenString()).SendOrErr(ctx)
//	if err != nil {
//		var apiErr *keboola.StorageError
//		if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.tokenInvalid" {
//			p.logger.Warnf(ctx, `token "%d" is not valid, creating a new: %s`, token.Token.ID, err)
//			return false, nil
//		} else {
//			p.logger.Warnf(ctx, `cannot verify token "%d": %s`, token.Token.ID, err)
//			return false, err
//		}
//	}
//	return true, nil
//}
//
//func (p *tableSinkPlugin) createToken(ctx context.Context, rb rollback.Builder, sinkKey key.SinkKey, bucketKey keboola.BucketKey) error {
//	// Prepare API request
//	permissions := keboola.BucketPermissions{bucketKey.BucketID: keboola.BucketPermissionWrite}
//	newTokenRequest := p.api.CreateTokenRequest(
//		keboola.WithDescription(
//			// Max length of description is 255 characters,
//			// this will be at most sourceID (48) + sinkID (48) + extra chars (24) = 120 characters.
//			fmt.Sprintf("[_internal] Stream Sink %s/%s", sinkKey.SourceID, sinkKey.SinkID),
//		),
//		keboola.WithBucketPermissions(permissions),
//		keboola.WithCanReadAllFileUploads(true),
//	)
//
//	// Create token
//	newToken, err := newTokenRequest.Send(ctx)
//	if err != nil {
//		err = errors.Errorf(`cannot create token with permissions to the bucket "%s": %w`, bucketKey.BucketID, err)
//		p.logger.Warn(ctx, err.Error())
//		return err
//	}
//
//	// Register rollback
//	rb.Add(func(ctx context.Context) error {
//		return p.api.DeleteTokenRequest(newToken.ID).SendOrErr(ctx)
//	})
//
//	// Save token
//	p.MergeOp(p.storage.Token().Put(sinkKey, *newToken))
//
//	p.logger.Infof(ctx, `created token "%d" with permissions to the bucket "%s"`, newToken.ID, bucketKey.BucketID)
//	return nil
//}
