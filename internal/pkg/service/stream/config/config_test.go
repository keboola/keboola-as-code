package config_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	local "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/config"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
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
# Unique ID of the node in the cluster. Validation rules: required
nodeID: ""
# Hostname for communication between nodes. Validation rules: required
hostname: ""
# Storage API host. Validation rules: required,hostname
storageApiHost: ""
pprof:
    # Enable PProf HTTP server. Don't use in the production.'
    enabled: false
    # Listen address of the PProf HTTP server. Validation rules: required,hostname_port
    listen: 0.0.0.0:4000
datadog:
    # Enable DataDog integration.
    enabled: true
    # Enable DataDog debug messages.
    debug: false
    profiler:
        # Enable DataDog profiler. Don't use in the production.
        enabled: false
        # Enable CPU profile.
        cpu: true
        # Enable memory profile.
        memory: true
        # Enable block profile, may have big overhead.
        block: false
        # Enable mutex profile, may have big overhead.
        mutex: false
        # Enable Goroutine profile, may have big overhead.
        goroutine: false
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
    publicUrl: http://localhost:8000
    task:
        # Defines time after the session is canceled if the client is unavailable. Validation rules: required
        ttlSeconds: 15
        # Enable periodical tasks cleanup functionality.
        cleanupEnabled: true
        # How often will old tasks be deleted. Validation rules: required
        cleanupInterval: 1h0m0s
distribution:
    # The maximum time to wait for creating a new session. Validation rules: required,minDuration=1s,maxDuration=1m
    grantTimeout: 5s
    # Timeout for the node registration to the cluster. Validation rules: required,minDuration=1s,maxDuration=5m
    startupTimeout: 1m0s
    # Timeout for the node un-registration from the cluster. Validation rules: required,minDuration=1s,maxDuration=5m
    shutdownTimeout: 10s
    # Interval of processing changes in the topology. Use 0 to disable the grouping. Validation rules: maxDuration=30s
    eventsGroupInterval: 5s
    # Seconds after which the node is automatically un-registered if an outage occurs. Validation rules: required,min=1,max=30
    ttlSeconds: 15
source:
    http:
        # Listen address of the HTTP source. Validation rules: required,hostname_port
        listen: 0.0.0.0:7000
        # Public URL of the HTTP source for link generation.
        publicUrl: null
        # HTTP request timeout. Validation rules: required,minDuration=1s,maxDuration=60s
        requestTimeout: 30s
        # TCP connection idle timeout. Validation rules: required,minDuration=1s,maxDuration=60s
        idleTimeout: 30s
        # The maximum number of concurrent connections the server may serve. Validation rules: required,min=100,max=1000000
        maxConnections: 200000
        # Read buffer size, all HTTP headers must fit in. Validation rules: required,minBytes=1kB,maxBytes=1MB
        readBufferSize: 16KB
        # Write buffer size. Validation rules: required,minBytes=1kB,maxBytes=1MB
        writeBufferSize: 4KB
        # Max size of the HTTP request body. Validation rules: required,minBytes=100B,maxBytes=4MB
        maxRequestBodySize: 1MB
sink:
    table:
        keboola:
            # Timeout to perform upload send event of slice or import event of file
            eventSendTimeout: 30s
            # Number of import jobs running in keboola for single sink
            jobLimit: 2
storage:
    # Mounted volumes path, each volume is in "{type}/{label}" subdir. Validation rules: required
    volumesPath: ""
    statistics:
        sync:
            # Statistics synchronization interval, from memory to the etcd. Validation rules: required,minDuration=100ms,maxDuration=5s
            interval: 1s
            # Statistics synchronization timeout. Validation rules: required,minDuration=1s,maxDuration=1m
            timeout: 30s
        cache:
            L2:
                # Enable statistics L2 in-memory cache, otherwise only L1 cache is used.
                enabled: true
                # Statistics L2 in-memory cache invalidation interval. Validation rules: required,minDuration=100ms,maxDuration=5s
                interval: 1s
    metadataCleanup:
        # Enable local storage metadata cleanup.
        enabled: true
        # Cleanup interval. Validation rules: required,minDuration=30s,maxDuration=24h
        interval: 30s
        # How many files are deleted in parallel. Validation rules: required,min=1,max=500
        concurrency: 50
        # Expiration interval of a file that has not yet been imported. Validation rules: required,minDuration=1h,maxDuration=720h,gtefield=ArchivedFileExpiration
        activeFileExpiration: 168h0m0s
        # Expiration interval of a file that has already been imported. Validation rules: required,minDuration=15m,maxDuration=720h
        archivedFileExpiration: 6h0m0s
    diskCleanup:
        # Enable local storage disks cleanup.
        enabled: true
        # Cleanup interval. Validation rules: required,minDuration=5m,maxDuration=24h
        interval: 5m0s
        # How many directories are removed in parallel. Validation rules: required,min=1,max=500
        concurrency: 50
    level:
        local:
            volume:
                assignment:
                    # Volumes count simultaneously utilized per sink. Validation rules: required,min=1,max=100
                    count: 1
                    # List of preferred volume types, start with the most preferred. Validation rules: required,min=1
                    preferredTypes:
                        - default
                registration:
                    # Number of seconds after the volume registration expires if the node is not available. Validation rules: required,min=1,max=60
                    ttlSeconds: 10
            encoding:
                encoder:
                    # Encoder type. Validation rules: required,oneof=csv
                    type: csv
                    # Concurrency of the format writer for the specified file type. 0 = auto = num of CPU cores. Validation rules: min=0,max=256
                    concurrency: 0
                    # Set's the limit of single row to be encoded. Limit should be bigger than accepted request on source otherwise received message will never be encoded. Validation rules: minBytes=1kB,maxBytes=2MB
                    rowSizeLimit: 1536KB
                # Max size of the buffer before compression, if compression is enabled. 0 = disabled. Validation rules: maxBytes=16MB
                inputBuffer: 2MB
                # Max size of a chunk sent over the network to a disk writer node. Validation rules: required,minBytes=64kB,maxBytes=1MB
                maxChunkSize: 512KB
                # If the defined number of chunks cannot be sent, the pipeline is marked as not ready. Validation rules: required,min=1,max=100
                failedChunksThreshold: 3
                compression:
                    # Compression type. Validation rules: required,oneof=none gzip
                    type: gzip
                    gzip:
                        # GZIP compression level: 1-9. Validation rules: min=1,max=9
                        level: 1
                        # GZIP implementation: standard, fast, parallel. Validation rules: required,oneof=standard fast parallel
                        implementation: parallel
                        # GZIP parallel block size. Validation rules: required,minBytes=16kB,maxBytes=100MB
                        blockSize: 256KB
                        # GZIP parallel concurrency, 0 = auto.
                        concurrency: 0
                sync:
                    # Sync mode: "cache" or "disk". Validation rules: required,oneof=disk cache
                    mode: disk
                    # Wait for sync to disk OS cache or to disk hardware, depending on the mode.
                    wait: true
                    # Minimal interval between syncs to disk. Validation rules: required,minDuration=1ms,maxDuration=30s
                    checkInterval: 5ms
                    # Written records count to trigger sync. Validation rules: required,min=1,max=1000000
                    countTrigger: 10000
                    # Size of buffered uncompressed data to trigger sync. Validation rules: required,minBytes=100B,maxBytes=500MB
                    uncompressedBytesTrigger: 1MB
                    # Size of buffered compressed data to trigger sync. Validation rules: required,minBytes=100B,maxBytes=100MB
                    compressedBytesTrigger: 256KB
                    # Interval from the last sync to trigger sync. Validation rules: required,minDuration=10ms,maxDuration=30s
                    intervalTrigger: 50ms
            writer:
                network:
                    # Listen address of the configuration HTTP API. Validation rules: required,hostname_port
                    listen: 0.0.0.0:6000
                    # Transport protocol. Validation rules: required,oneof=tcp kcp
                    transport: tcp
                    # Keep alive interval. Validation rules: required,minDuration=1s,maxDuration=60s
                    keepAliveInterval: 5s
                    # Minimum number of slices opened per source node, up to the count of assigned volumes. Validation rules: required,min=1,max=20
                    minSlicesPerSourceNode: 2
                    # How many streams may be waiting an accept per connection. Validation rules: required,min=10,max=100000
                    maxWaitingStreamsPerConn: 1024
                    # Validation rules: required,minBytes=256kB,maxBytes=10MB
                    streamMaxWindow: 8MB
                    # Stream ACK timeout. Validation rules: required,minDuration=1s,maxDuration=30s
                    streamOpenTimeout: 10s
                    # Stream close timeout. Validation rules: required,minDuration=1s,maxDuration=30s
                    streamCloseTimeout: 10s
                    # Stream write timeout. Validation rules: required,minDuration=1s,maxDuration=60s
                    streamWriteTimeout: 10s
                    # How long the server waits for streams closing. Validation rules: required,minDuration=1s,max=600s
                    shutdownTimeout: 30s
                    # Buffer size for transferring data between source and writer nodes (kcp). Validation rules: required,minBytes=16kB,maxBytes=100MB
                    kcpInputBuffer: 8MB
                    # Buffer size for transferring responses between writer and source node (kcp). Validation rules: required,minBytes=16kB,maxBytes=100MB
                    kcpResponseBuffer: 512KB
                    # Pipeline balancer type which balances the writing on particular nodes based on selected strategy. Validation rules: required,oneof=rand roundRobin
                    pipelineBalancer: roundRobin
                allocation:
                    # Allocate disk space for each slice. Useless for SSD.
                    enabled: true
                    # Size of allocated disk space for a slice. Validation rules: required
                    static: 100MB
                    # Allocate disk space as % from the previous slice size. Validation rules: min=100,max=500
                    relative: 110
        staging:
            operator:
                # Upload triggers check interval. Validation rules: required,minDuration=100ms,maxDuration=30s
                sliceRotationCheckInterval: 1s
                # Timeout of the slice rotation operation. Validation rules: required,minDuration=30s,maxDuration=15m
                sliceRotationTimeout: 5m0s
                # Timeout of the slice close operation. Validation rules: required,minDuration=10s,maxDuration=10m
                sliceCloseTimeout: 1m0s
                # Interval of checking slices in the 'uploading' state to perform upload. Validation rules: required,minDuration=500ms,maxDuration=30s
                sliceUploadCheckInterval: 2s
                # Timeout of the slice upload operation. Validation rules: required,minDuration=30s,maxDuration=60m
                sliceUploadTimeout: 15m0s
            upload:
                # Min duration from the last upload to trigger the next, takes precedence over other settings. Validation rules: required,minDuration=1s,maxDuration=30m
                minInterval: 10s
                trigger:
                    # Records count to trigger slice upload. Validation rules: required,min=1,max=10000000
                    count: 10000
                    # Records size to trigger slice upload. Validation rules: required,minBytes=100B,maxBytes=50MB
                    size: 5MB
                    # Duration from the last slice upload to trigger the next upload. Validation rules: required,minDuration=1s,maxDuration=30m
                    interval: 30s
        target:
            operator:
                # Import triggers check interval. Validation rules: required,minDuration=100ms,maxDuration=30s
                fileRotationCheckInterval: 1s
                # Timeout of the file rotation operation. Validation rules: required,minDuration=30s,maxDuration=15m
                fileRotationTimeout: 5m0s
                # Timeout of the file close operation. Validation rules: required,minDuration=10s,maxDuration=10m
                fileCloseTimeout: 1m0s
                # Interval of checking files in the importing state. Validation rules: required,minDuration=500ms,maxDuration=30s
                fileImportCheckInterval: 1s
                # Timeout of the file import operation. Validation rules: required,minDuration=30s,maxDuration=60m
                fileImportTimeout: 15m0s
            import:
                # Min duration from the last import to trigger the next, takes precedence over other settings. Validation rules: required,minDuration=30s,maxDuration=24h
                minInterval: 1m0s
                trigger:
                    # Records count to trigger file import. Validation rules: required,min=1,max=10000000
                    count: 50000
                    # Records size to trigger file import. Validation rules: required,minBytes=100B,maxBytes=500MB
                    size: 50MB
                    # Duration from the last import to trigger the next import. Validation rules: required,minDuration=30s,maxDuration=24h
                    interval: 1m0s
                    # Number of slices in the file to trigger file import. Validation rules: required,min=1,max=1000
                    slicesCount: 100
                    # Min remaining expiration to trigger file import. Validation rules: required,minDuration=5m,maxDuration=45m
                    expiration: 30m0s
encryption:
    # Encryption provider. Validation rules: required,oneof=none native gcp aws azure
    provider: none
    native:
        # Secret key for local encryption. Do not use in production.
        secretKey: '*****'
    gcp:
        # Key ID in Google Cloud Key Management Service. Validation rules: required
        kmsKeyId: ""
    aws:
        # AWS Region. Validation rules: required
        region: ""
        # Key ID in AWS Key Management Service. Validation rules: required
        kmsKeyId: ""
    azure:
        # Azure Key Vault URL. Validation rules: required,url
        keyVaultUrl: ""
        # Key name in the vault. Validation rules: required
        keyName: ""
`), strings.TrimSpace(string(bytes)))

	// Add missing values, and validate it
	cfg.NodeID = "test-node"
	cfg.Hostname = "localhost"
	cfg.StorageAPIHost = "connection.keboola.local"
	cfg.Storage.VolumesPath = "/tmp/stream-volumes"
	cfg.Source.HTTP.PublicURL, _ = url.Parse("https://stream-in.keboola.local")
	cfg.Etcd.Endpoint = "test-etcd"
	cfg.Etcd.Namespace = "test-namespace"
	cfg.Encryption.Provider = encryption.ProviderNative
	cfg.Encryption.Native.SecretKey = []byte("12345678901234567890123456789012")
	cfg.Encryption.Normalize()
	require.NoError(t, validator.New().Validate(context.Background(), cfg))
}

func TestTableSinkConfigPatch_ToKVs(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpAll(
		config.New(),
		config.Patch{
			Storage: &storage.ConfigPatch{
				Level: &level.ConfigPatch{
					Local: &local.ConfigPatch{
						Encoding: &encoding.ConfigPatch{
							MaxChunkSize: ptr.Ptr(123 * datasize.KB),
						},
					},
				},
			},
		},
		configpatch.WithModifyProtected(),
	)
	require.NoError(t, err)

	assert.JSONEq(t, strings.TrimSpace(`
[
  {
    "key": "storage.level.local.encoding.compression.gzip.blockSize",
    "type": "string",
    "description": "GZIP parallel block size.",
    "value": "256KB",
    "defaultValue": "256KB",
    "overwritten": false,
    "protected": true,
    "validation": "required,minBytes=16kB,maxBytes=100MB"
  },
  {
    "key": "storage.level.local.encoding.compression.gzip.concurrency",
    "type": "int",
    "description": "GZIP parallel concurrency, 0 = auto.",
    "value": 0,
    "defaultValue": 0,
    "overwritten": false,
    "protected": true
  },
  {
    "key": "storage.level.local.encoding.compression.gzip.implementation",
    "type": "string",
    "description": "GZIP implementation: standard, fast, parallel.",
    "value": "parallel",
    "defaultValue": "parallel",
    "overwritten": false,
    "protected": true,
    "validation": "required,oneof=standard fast parallel"
  },
  {
    "key": "storage.level.local.encoding.compression.gzip.level",
    "type": "int",
    "description": "GZIP compression level: 1-9.",
    "value": 1,
    "defaultValue": 1,
    "overwritten": false,
    "protected": true,
    "validation": "min=1,max=9"
  },
  {
    "key": "storage.level.local.encoding.compression.type",
    "type": "string",
    "description": "Compression type.",
    "value": "gzip",
    "defaultValue": "gzip",
    "overwritten": false,
    "protected": true,
    "validation": "required,oneof=none gzip"
  },
  {
    "key": "storage.level.local.encoding.encoder.concurrency",
    "type": "int",
    "description": "Concurrency of the format writer for the specified file type. 0 = auto = num of CPU cores",
    "value": 0,
    "defaultValue": 0,
    "overwritten": false,
    "protected": true,
    "validation": "min=0,max=256"
  },
  {
    "key": "storage.level.local.encoding.encoder.type",
    "type": "string",
    "description": "Encoder type.",
    "value": "csv",
    "defaultValue": "csv",
    "overwritten": false,
    "protected": true,
    "validation": "required,oneof=csv"
  },
  {
    "key": "storage.level.local.encoding.failedChunksThreshold",
    "type": "int",
    "description": "If the defined number of chunks cannot be sent, the pipeline is marked as not ready.",
    "value": 3,
    "defaultValue": 3,
    "overwritten": false,
    "protected": true,
    "validation": "required,min=1,max=100"
  },
  {
    "key": "storage.level.local.encoding.inputBuffer",
    "type": "string",
    "description": "Max size of the buffer before compression, if compression is enabled. 0 = disabled",
    "value": "2MB",
    "defaultValue": "2MB",
    "overwritten": false,
    "protected": true,
    "validation": "maxBytes=16MB"
  },
  {
    "key": "storage.level.local.encoding.maxChunkSize",
    "type": "string",
    "description": "Max size of a chunk sent over the network to a disk writer node.",
    "value": "123KB",
    "defaultValue": "512KB",
    "overwritten": true,
    "protected": true,
    "validation": "required,minBytes=64kB,maxBytes=1MB"
  },
  {
    "key": "storage.level.local.encoding.sync.checkInterval",
    "type": "string",
    "description": "Minimal interval between syncs to disk.",
    "value": "5ms",
    "defaultValue": "5ms",
    "overwritten": false,
    "protected": true,
    "validation": "required,minDuration=1ms,maxDuration=30s"
  },
  {
    "key": "storage.level.local.encoding.sync.compressedBytesTrigger",
    "type": "string",
    "description": "Size of buffered compressed data to trigger sync.",
    "value": "256KB",
    "defaultValue": "256KB",
    "overwritten": false,
    "protected": true,
    "validation": "required,minBytes=100B,maxBytes=100MB"
  },
  {
    "key": "storage.level.local.encoding.sync.countTrigger",
    "type": "uint",
    "description": "Written records count to trigger sync.",
    "value": 10000,
    "defaultValue": 10000,
    "overwritten": false,
    "protected": true,
    "validation": "required,min=1,max=1000000"
  },
  {
    "key": "storage.level.local.encoding.sync.intervalTrigger",
    "type": "string",
    "description": "Interval from the last sync to trigger sync.",
    "value": "50ms",
    "defaultValue": "50ms",
    "overwritten": false,
    "protected": true,
    "validation": "required,minDuration=10ms,maxDuration=30s"
  },
  {
    "key": "storage.level.local.encoding.sync.mode",
    "type": "string",
    "description": "Sync mode: \"cache\" or \"disk\".",
    "value": "disk",
    "defaultValue": "disk",
    "overwritten": false,
    "protected": true,
    "validation": "required,oneof=disk cache"
  },
  {
    "key": "storage.level.local.encoding.sync.uncompressedBytesTrigger",
    "type": "string",
    "description": "Size of buffered uncompressed data to trigger sync.",
    "value": "1MB",
    "defaultValue": "1MB",
    "overwritten": false,
    "protected": true,
    "validation": "required,minBytes=100B,maxBytes=500MB"
  },
  {
    "key": "storage.level.local.encoding.sync.wait",
    "type": "bool",
    "description": "Wait for sync to disk OS cache or to disk hardware, depending on the mode.",
    "value": true,
    "defaultValue": true,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.level.local.volume.assignment.count",
    "type": "int",
    "description": "Volumes count simultaneously utilized per sink.",
    "value": 1,
    "defaultValue": 1,
    "overwritten": false,
    "protected": true,
    "validation": "required,min=1,max=100"
  },
  {
    "key": "storage.level.local.volume.assignment.preferredTypes",
    "type": "[]string",
    "description": "List of preferred volume types, start with the most preferred.",
    "value": [
      "default"
    ],
    "defaultValue": [
      "default"
    ],
    "overwritten": false,
    "protected": true,
    "validation": "required,min=1"
  },
  {
    "key": "storage.level.staging.upload.trigger.count",
    "type": "uint64",
    "description": "Records count to trigger slice upload.",
    "value": 10000,
    "defaultValue": 10000,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=10000000"
  },
  {
    "key": "storage.level.staging.upload.trigger.interval",
    "type": "string",
    "description": "Duration from the last slice upload to trigger the next upload.",
    "value": "30s",
    "defaultValue": "30s",
    "overwritten": false,
    "protected": false,
    "validation": "required,minDuration=1s,maxDuration=30m"
  },
  {
    "key": "storage.level.staging.upload.trigger.size",
    "type": "string",
    "description": "Records size to trigger slice upload.",
    "value": "5MB",
    "defaultValue": "5MB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=100B,maxBytes=50MB"
  },
  {
    "key": "storage.level.target.import.trigger.count",
    "type": "uint64",
    "description": "Records count to trigger file import.",
    "value": 50000,
    "defaultValue": 50000,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=10000000"
  },
  {
    "key": "storage.level.target.import.trigger.expiration",
    "type": "string",
    "description": "Min remaining expiration to trigger file import.",
    "value": "30m0s",
    "defaultValue": "30m0s",
    "overwritten": false,
    "protected": true,
    "validation": "required,minDuration=5m,maxDuration=45m"
  },
  {
    "key": "storage.level.target.import.trigger.interval",
    "type": "string",
    "description": "Duration from the last import to trigger the next import.",
    "value": "1m0s",
    "defaultValue": "1m0s",
    "overwritten": false,
    "protected": false,
    "validation": "required,minDuration=30s,maxDuration=24h"
  },
  {
    "key": "storage.level.target.import.trigger.size",
    "type": "string",
    "description": "Records size to trigger file import.",
    "value": "50MB",
    "defaultValue": "50MB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=100B,maxBytes=500MB"
  },
  {
    "key": "storage.level.target.import.trigger.slicesCount",
    "type": "uint64",
    "description": "Number of slices in the file to trigger file import.",
    "value": 100,
    "defaultValue": 100,
    "overwritten": false,
    "protected": true,
    "validation": "required,min=1,max=1000"
  }
]
`), strings.TrimSpace(json.MustEncodeString(kvs, true)))
}

func TestConfig_BindKVs_Ok(t *testing.T) {
	t.Parallel()

	patch := config.Patch{}
	require.NoError(t, configpatch.BindKVs(&patch, []configpatch.PatchKV{
		{
			KeyPath: "storage.level.local.encoding.maxChunkSize",
			Value:   "456kB",
		},
	}))

	assert.Equal(t, config.Patch{
		Storage: &storage.ConfigPatch{
			Level: &level.ConfigPatch{
				Local: &local.ConfigPatch{
					Encoding: &encoding.ConfigPatch{
						MaxChunkSize: ptr.Ptr(456 * datasize.KB),
					},
				},
			},
		},
	}, patch)
}

func TestConfig_BindKVs_InvalidType(t *testing.T) {
	t.Parallel()

	err := configpatch.BindKVs(&config.Patch{}, []configpatch.PatchKV{
		{
			KeyPath: "storage.level.local.encoding.compression.gzip.level",
			Value:   "foo",
		},
	})

	if assert.Error(t, err) {
		assert.Equal(t, `invalid "storage.level.local.encoding.compression.gzip.level" value: found type "string", expected "int"`, err.Error())
	}
}

func TestConfig_BindKVs_InvalidValue(t *testing.T) {
	t.Parallel()

	err := configpatch.BindKVs(&config.Patch{}, []configpatch.PatchKV{
		{
			KeyPath: "storage.level.local.encoding.maxChunkSize",
			Value:   "foo",
		},
	})

	if assert.Error(t, err) {
		assert.Equal(t, `invalid "storage.level.local.encoding.maxChunkSize" value "foo": invalid syntax`, err.Error())
	}
}
