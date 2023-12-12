package config_test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestConfig_DefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := config.New()
	bytes, err := configmap.NewDumper().Dump(&cfg).AsYAML()
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(`
# Enable logging at DEBUG level.
debugLog: false
# Log HTTP client requests and responses as debug messages.
debugHTTPClient: false
# Path where CPU profile is saved.
cpuProfilePath: ""
# Unique ID of the node in the cluster. Validation rules: required
nodeID: ""
datadog:
    # Enable DataDog integration.
    enabled: true
    # Enable DataDog debug messages.
    debug: false
etcd:
    # Etcd endpoint. Validation rules: required
    endpoint: ""
    # Etcd namespace. Validation rules: required
    namespace: ""
    # Etcd username.
    username: ""
    # Etcd password.
    password: '*****'
    # Etcd connect timeout. Validation rules: required
    connectTimeout: 30s
    # Etcd keep alive timeout. Validation rules: required
    keepAliveTimeout: 5s
    # Etcd keep alive interval. Validation rules: required
    keepAliveInterval: 10s
    # Etcd operations logging as debug messages.
    debugLog: false
metrics:
    # Prometheus scraping metrics listen address. Validation rules: required,hostname_port
    listen: 0.0.0.0:9000
api:
    # Listen address of the configuration HTTP API. Validation rules: required,hostname_port
    listen: 0.0.0.0:8000
    # Public URL of the configuration HTTP API for link generation. Validation rules: required
    publicURL: http://localhost:8000
source:
    http:
        # Listen address of the HTTP source. Validation rules: required,hostname_port
        listen: 0.0.0.0:7000
sink:
    table:
        statistics:
            sync:
                # Statistics synchronization interval, from memory to the etcd. Validation rules: required,minDuration=100ms,maxDuration=5s
                interval: 1s
                # Statistics synchronization timeout. Validation rules: required,minDuration=1s,maxDuration=1m
                timeout: 30s
            cache:
                # Statistics L2 in-memory cache invalidation interval. Validation rules: required,minDuration=100ms,maxDuration=5s
                invalidationInterval: 1s
        storage:
            local:
                compression:
                    # Compression type. Validation rules: required,oneof=none gzip zstd
                    type: gzip
                    gzip:
                        # GZIP compression level: 1-9. Validation rules: min=1,max=9
                        level: 1
                        # GZIP implementation: standard, fast, parallel. Validation rules: required,oneof=standard fast parallel
                        implementation: parallel
                        # GZIP parallel block size. Validation rules: required,min=16384,max=104857600
                        blockSize: 256KB
                        # GZIP parallel concurrency, 0 = auto.
                        concurrency: 0
                    zstd:
                        # ZSTD compression level: fastest, default, better, best. Validation rules: min=1,max=4
                        level: fastest
                        # ZSTD window size. Validation rules: required,min=1024,max=536870912
                        windowSize: 1MB
                        # ZSTD concurrency, 0 = auto
                        concurrency: 0
                volumesAssignment:
                    # Volumes simultaneously utilized per pod and sink. Validation rules: required,min=1
                    perPod: 1
                    # List of preferred volume types, start with the most preferred.
                    preferredTypes:
                        - default
                diskSync:
                    # Sync mode: "disabled", "cache" or "disk". Validation rules: required,oneof=disabled disk cache
                    mode: disk
                    # Wait for sync to disk OS cache or to disk hardware, depending on the mode. Validation rules: excluded_if= Mode disabled
                    wait: true
                    # Minimal interval between syncs. Validation rules: min=0,maxDuration=2s,excluded_if=Mode disabled,required_if=Mode disk,required_if=Mode cache
                    checkInterval: 5ms
                    # Written records count to trigger sync. Validation rules: max=1000000,excluded_if=Mode disabled,required_if=Mode disk,required_if=Mode cache
                    countTrigger: 500
                    # Written size to trigger sync. Validation rules: maxBytes=100MB,excluded_if=Mode disabled,required_if=Mode disk,required_if=Mode cache
                    bytesTrigger: 1MB
                    # Interval from the last sync to trigger sync. Validation rules: min=0,maxDuration=2s,excluded_if=Mode disabled,required_if=Mode disk,required_if=Mode cache
                    intervalTrigger: 50ms
                diskAllocation:
                    # Allocate disk space for each slice.
                    enabled: true
                    # Size of allocated disk space for a slice.
                    size: 100MB
                    # Allocate disk space as % from the previous slice size. Validation rules: min=0,max=200
                    sizePercent: 110
            staging:
                upload:
                    # Minimal interval between uploads. Validation rules: required,minDuration=1s,maxDuration=5m
                    minInterval: 5s
                    trigger:
                        # Records count. Validation rules: required,min=1,max=10000000
                        count: 10000
                        # Records size. Validation rules: required,minBytes=100B,maxBytes=50MB
                        size: 1MB
                        # Duration from the last upload. Validation rules: required,minDuration=1s,maxDuration=30m
                        interval: 1m0s
            target:
                import:
                    # Minimal interval between imports. Validation rules: required,minDuration=30s,maxDuration=30m
                    minInterval: 1m0s
                    trigger:
                        # Records count. Validation rules: required,min=1,max=10000000
                        count: 50000
                        # Records size. Validation rules: required,minBytes=100B,maxBytes=500MB
                        size: 5MB
                        # Duration from the last import. Validation rules: required,minDuration=60s,maxDuration=24h
                        interval: 5m0s
`), strings.TrimSpace(string(bytes)))
}
