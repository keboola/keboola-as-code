package cleanup

import (
	"context"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
)

const BoundaryInDays = 14

type Cleanup struct {
	client *etcd.Client
	clock  clock.Clock
	logger log.Logger
	schema *schema.Schema
	store  *store.Store
}

func New(client *etcd.Client, clock clock.Clock, logger log.Logger, schema *schema.Schema, store *store.Store) *Cleanup {
	return &Cleanup{
		client: client,
		clock:  clock,
		logger: logger,
		schema: schema,
		store:  store,
	}
}

func (c *Cleanup) Run(ctx context.Context, receiver model.Receiver) (err error) {
	c.cleanupTasks(ctx, receiver.ReceiverKey)

	for _, e := range receiver.Exports {
		c.cleanupFiles(ctx, e.ExportKey)
	}

	return nil
}

func (c *Cleanup) cleanupTasks(ctx context.Context, receiverKey key.ReceiverKey) {
	tasks, err := c.schema.Tasks().InReceiver(receiverKey).GetAll().Do(ctx, c.client).All()
	if err != nil {
		c.logger.Error(err)
		return
	}
	for _, t := range tasks {
		if !t.Value.IsForCleanup() {
			continue
		}

		_, err = c.schema.Tasks().ByKey(t.Value.TaskKey).Delete().Do(ctx, c.client)
		if err != nil {
			c.logger.Error(err)
		}
		c.logger.Infof(`deleting task "%s"`, t.Value.TaskKey.String())
	}
}

func (c *Cleanup) cleanupFiles(ctx context.Context, exportKey key.ExportKey) {
	now := c.clock.Now()
	boundaryTime := now.AddDate(0, 0, -BoundaryInDays)
	for _, fileState := range filestate.All() {
		fileBoundKey := key.UTCTime(boundaryTime).String()
		files, err := c.store.ListFilesInState(ctx, fileState, exportKey, iterator.WithEnd(fileBoundKey))
		if err != nil {
			c.logger.Error(err)
		}

		for _, file := range files {
			// Delete slices
			for _, sliceState := range slicestate.All() {
				slices, err := c.schema.Slices().InState(sliceState).InFile(file.FileKey).GetAll().Do(ctx, c.client).All()
				if err != nil {
					c.logger.Error(err)
					continue
				}
				for _, slice := range slices {
					// Delete records
					_, err := c.schema.Records().InSlice(slice.Value.SliceKey).DeleteAll().Do(ctx, c.client)
					if err != nil {
						c.logger.Error(err)
					}

					// Delete the slice
					_, err = c.schema.Slices().InState(sliceState).ByKey(slice.Value.SliceKey).Delete().Do(ctx, c.client)
					if err != nil {
						c.logger.Error(err)
					}
				}
			}

			// Delete the file
			_, err := c.schema.Files().InState(fileState).ByKey(file.FileKey).Delete().Do(ctx, c.client)
			if err != nil {
				c.logger.Error(err)
			}
			c.logger.Infof(`deleting file "%s"`, file.FileKey.String())
		}
	}
}
