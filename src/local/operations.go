package local

import (
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/diff"
)

func Save(result *diff.Result, logger *zap.SugaredLogger, workers *errgroup.Group) error {
	return nil
}

func Delete(result *diff.Result, logger *zap.SugaredLogger, workers *errgroup.Group) error {
	return nil
}
