package main

import (
	"context"
	"flag"
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/migrate/core"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Flags struct {
	storageAPIToken string
	host            string
}

func NewFlags() Flags {
	storageAPIToken := flag.String("storage-api-token", "", "storage api token")
	host := flag.String("host", "", "host")

	flag.Parse()

	return Flags{*storageAPIToken, *host}
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	logger := log.NewServiceLogger(os.Stdout, true)

	flags := NewFlags()
	if flags.host == "" && flags.storageAPIToken == "" {
		logger.Error(ctx, "host/storage-api-token token is required")
		return errors.New("host/storage-api-token is required")
	}

	logger.Info(ctx, "Starting migration...")
	// fetch receivers and exports from old API
	bufferReceivers, err := core.FetchBufferReceivers(ctx, flags.host, flags.storageAPIToken)
	if err != nil {
		logger.Error(ctx, err.Error())
		return err
	}

	for _, receiver := range bufferReceivers.Receivers {
		err = receiver.CreateSource(ctx, flags.storageAPIToken, flags.host)
		if err != nil {
			logger.Error(ctx, err.Error())
		} else {
			logger.Infof(ctx, `Source "%s" with id "%s" was created`, receiver.Name, receiver.ID)
		}

		for _, export := range receiver.Exports {
			if err = export.CreateSink(ctx, flags.storageAPIToken, flags.host); err != nil {
				logger.Error(ctx, err.Error())
			} else {
				logger.Infof(ctx, `Sink "%s" with id "%s" was created`, export.Name, export.ID)
			}
		}
	}

	logger.Info(ctx, "Migration done")

	return nil
}
