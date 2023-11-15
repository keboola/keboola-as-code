package config_test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestConfig_Flags(t *testing.T) {
	t.Parallel()

	cfg := config.New()
	fs := pflag.NewFlagSet("app", pflag.ContinueOnError)
	require.NoError(t, configmap.StructToFlags(fs, &cfg, nil))
	assert.Equal(t, strings.TrimSpace(`
      --storage-local-compression-gzip-block-size string           GZIP parallel block size. (default "256KB")
      --storage-local-compression-gzip-concurrency int             GZIP parallel concurrency, 0 = auto.
      --storage-local-compression-gzip-implementation string       GZIP implementation: standard, fast, parallel. (default "parallel")
      --storage-local-compression-gzip-level int                   GZIP compression level: 1-9 (default 1)
      --storage-local-compression-type string                      Compression type. (default "gzip")
      --storage-local-compression-zstd-concurrency int             ZSTD concurrency, 0 = auto
      --storage-local-compression-zstd-level string                ZSTD compression level: fastest, default, better, best (default "fastest")
      --storage-local-compression-zstd-window-size string          ZSTD window size. (default "1MB")
      --storage-local-disk-allocation-enabled                      Allocate disk space for each slice. (default true)
      --storage-local-disk-allocation-size string                  Size of allocated disk space for a slice. (default "100MB")
      --storage-local-disk-allocation-size-percent int             Allocate disk space as % from the previous slice size. (default 110)
      --storage-local-disk-sync-bytes-trigger string               Written size to trigger sync. (default "1MB")
      --storage-local-disk-sync-check-interval string              Minimal interval between syncs. (default "5ms")
      --storage-local-disk-sync-count-trigger uint                 Written records count to trigger sync. (default 500)
      --storage-local-disk-sync-interval-trigger string            Interval from the last sync after the sync will be triggered. (default "50ms")
      --storage-local-disk-sync-mode string                        Sync mode: "disabled", "cache" or "disk". (default "disk")
      --storage-local-disk-sync-wait                               Wait for sync to disk cache or to disk, depending on the mode. (default true)
      --storage-local-volumes-assignment-per-pod int               Volumes simultaneously utilized per pod and sink. (default 1)
      --storage-local-volumes-assignment-preferred-types strings   List of preferred volume types, from most preferred. (default [default])
`), strings.TrimSpace(fs.FlagUsages()))
}

func TestConfig_DefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := config.New()
	bytes, err := configmap.NewDumper().Dump(&cfg).AsYAML()
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(`
storage:
    local:
        compression:
            # Compression type.
            type: gzip
            gzip:
                # GZIP compression level: 1-9
                level: 1
                # GZIP implementation: standard, fast, parallel.
                implementation: parallel
                # GZIP parallel block size.
                blockSize: 256KB
                # GZIP parallel concurrency, 0 = auto.
                concurrency: 0
            zstd:
                # ZSTD compression level: fastest, default, better, best
                level: fastest
                # ZSTD window size.
                windowSize: 1MB
                # ZSTD concurrency, 0 = auto
                concurrency: 0
        volumesAssignment:
            # Volumes simultaneously utilized per pod and sink.
            perPod: 1
            # List of preferred volume types, from most preferred.
            preferredTypes:
                - default
        diskSync:
            # Sync mode: "disabled", "cache" or "disk".
            mode: disk
            # Wait for sync to disk cache or to disk, depending on the mode.
            wait: true
            # Minimal interval between syncs.
            checkInterval: 5ms
            # Written records count to trigger sync.
            countTrigger: 500
            # Written size to trigger sync.
            bytesTrigger: 1MB
            # Interval from the last sync after the sync will be triggered.
            intervalTrigger: 50ms
        diskAllocation:
            # Allocate disk space for each slice.
            enabled: true
            # Size of allocated disk space for a slice.
            size: 100MB
            # Allocate disk space as % from the previous slice size.
            sizePercent: 110
`), strings.TrimSpace(string(bytes)))
}
