package file

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) closeFileOnSinkDeactivation() {
	r.plugins.Collection().OnSinkDeactivation(func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) {
	})
}

func (r *Repository) close(ctx context.Context, now time.Time, file model.File) (*op.TxnOp[model.File], error) {
	// Switch the old file from the state model.FileWriting to the state model.FileClosing
	updated, err := file.WithState(now, model.FileClosing)
	if err != nil {
		return nil, err
	}

	// Save update old file
	return r.save(ctx, now, &file, &updated), nil
}
