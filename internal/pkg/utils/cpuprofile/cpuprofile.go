package cpuprofile

import (
	"os"
	"runtime/pprof"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func Start(filePath string, logger log.Logger) (stop func()) {
	logger = logger.AddPrefix("[cpu-profile]")

	f, err := os.Create(filePath) //nolint: forbidigo
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		logger.Error(err)
		os.Exit(1)
	}

	logger.Info("started")
	return func() {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil { //nolint: forbidigo
			logger.Error(err)
			os.Exit(1)
		}
		logger.Info("stopped")
	}
}
