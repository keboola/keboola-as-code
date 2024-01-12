package cpuprofile

import (
	"context"
	"os"
	"runtime/pprof"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func Start(ctx context.Context, filePath string, logger log.Logger) (stop func(), err error) {
	logger = logger.WithComponent("cpu-profile")

	f, err := os.Create(filePath) //nolint: forbidigo
	if err != nil {
		return nil, err
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		return nil, err
	}

	logger.InfoCtx(ctx, "started")
	return func() {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil { //nolint: forbidigo
			logger.ErrorCtx(ctx, err)
			os.Exit(1)
		}
		logger.InfoCtx(ctx, "stopped")
	}, nil
}
