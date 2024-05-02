package main

import (
	"context"
	"flag"
	"os"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/migrate/source"
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
	ctx := context.Background()
	logger := log.NewServiceLogger(os.Stdout, true)
	wg := sync.WaitGroup{}

	flags := NewFlags()
	if flags.host == "" && flags.storageAPIToken == "" {
		logger.Error(ctx, "usage: migrate storage-api-token host")
		os.Exit(1)
	}

	// fetch receivers and exports from old API
	bufferReceivers, err := source.FetchBufferReceivers(ctx, flags.host, flags.storageAPIToken, logger)
	if err != nil {
		logger.Error(ctx, err.Error())
		os.Exit(1)
	}

	logger.Info(ctx, `Starting migration...`)
	for _, receiver := range bufferReceivers.Receivers {
		wg.Add(1)
		go func() {
			err = receiver.CreateSource(ctx, flags.storageAPIToken, flags.host, &wg, logger)
			if err != nil {
				logger.Error(ctx, err.Error())
			}
		}()
	}
	wg.Wait()

	logger.Info(ctx, `Migration was finished.`)
}
