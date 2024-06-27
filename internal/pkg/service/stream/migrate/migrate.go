package main

import (
	"context"
	"flag"
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/migrate/source"
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
		return errors.New("host/storageAPIToken are required")
	}

	logger.Info(ctx, "Starting migration...")
	// fetch receivers and exports from old API
	bufferReceivers, err := source.FetchBufferReceivers(ctx, flags.host, flags.storageAPIToken)
	if err != nil {
		logger.Error(ctx, err.Error())
		return err
	}

	for _, receiver := range bufferReceivers.Receivers {
		err = receiver.CreateSource(ctx, flags.storageAPIToken, flags.host)
		if err != nil {
			logger.Error(ctx, err.Error())
		}

	}
	logger.Info(ctx, "Migration done")
	return nil
}
