package plugin

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
	"gocloud.dev/blob"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink/bridge/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type uploadSliceFn func(volume *diskreader.Volume, slice *model.Slice, sinkSchema schema.Schema, client etcd.KV) (*blob.Writer, diskreader.Reader, error)

func (p *Plugins) RegisterSliceUploader(fn uploadSliceFn) {
	p.sliceUploader = fn
	// p.sliceUploader = keboola.NewUploadSliceWriter
}

func (p *Plugins) UploadSlice(
	ctx context.Context,
	volume *diskreader.Volume,
	slice *model.Slice,
	sinkSchema schema.Schema,
	client etcd.KV,
) error {
	var err error
	uploader, reader, err := p.sliceUploader(volume, slice, sinkSchema, client)
	if err != nil {
		return err
	}

	defer func() {
		err = reader.Close(ctx)
	}()

	_, err = reader.WriteTo(uploader)
	if err != nil {
		return err
	}

	/*if errors.As(err, &pipeline.NoOpenerFoundError{}) {
		continue
	}*/

	return err
}
