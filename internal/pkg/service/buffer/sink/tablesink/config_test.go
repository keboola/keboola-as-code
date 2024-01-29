package tablesink_test

import (
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
)

func TestConfig_ToKVs(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpKVs(
		tablesink.NewConfig(),
		tablesink.ConfigPatch{
			Storage: &storage.ConfigPatch{
				Local: &local.ConfigPatch{
					DiskAllocation: &diskalloc.ConfigPatch{
						Size: test.Ptr(456 * datasize.MB),
					},
				},
			},
		},
	)
	require.NoError(t, err)

	assert.Equal(t, strings.TrimSpace(`
[
  {
    "key": "storage.local.compression.gzip.blockSize",
    "value": "256KB",
    "defaultValue": "256KB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=16kB,maxBytes=100MB"
  },
  {
    "key": "storage.local.compression.gzip.concurrency",
    "value": 0,
    "defaultValue": 0,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.local.compression.gzip.implementation",
    "value": "parallel",
    "defaultValue": "parallel",
    "overwritten": false,
    "protected": false,
    "validation": "required,oneof=standard fast parallel"
  },
  {
    "key": "storage.local.compression.gzip.level",
    "value": 1,
    "defaultValue": 1,
    "overwritten": false,
    "protected": false,
    "validation": "min=1,max=9"
  },
  {
    "key": "storage.local.compression.type",
    "value": "gzip",
    "defaultValue": "gzip",
    "overwritten": false,
    "protected": false,
    "validation": "required,oneof=none gzip zstd"
  },
  {
    "key": "storage.local.compression.zstd.concurrency",
    "value": 0,
    "defaultValue": 0,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.local.compression.zstd.level",
    "value": 1,
    "defaultValue": 1,
    "overwritten": false,
    "protected": false,
    "validation": "min=1,max=4"
  },
  {
    "key": "storage.local.compression.zstd.windowSize",
    "value": "1MB",
    "defaultValue": "1MB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=1kB,maxBytes=512MB"
  },
  {
    "key": "storage.local.diskAllocation.enabled",
    "value": true,
    "defaultValue": true,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.local.diskAllocation.size",
    "value": "456MB",
    "defaultValue": "100MB",
    "overwritten": true,
    "protected": false,
    "validation": "required"
  },
  {
    "key": "storage.local.diskAllocation.sizePercent",
    "value": 110,
    "defaultValue": 110,
    "overwritten": false,
    "protected": false,
    "validation": "min=100,max=500"
  },
  {
    "key": "storage.local.diskSync.bytesTrigger",
    "value": "1MB",
    "defaultValue": "1MB",
    "overwritten": false,
    "protected": false,
    "validation": "maxBytes=100MB,required_if=Mode disk,required_if=Mode cache"
  },
  {
    "key": "storage.local.diskSync.checkInterval",
    "value": "5ms",
    "defaultValue": "5ms",
    "overwritten": false,
    "protected": false,
    "validation": "min=0,maxDuration=2s,required_if=Mode disk,required_if=Mode cache"
  },
  {
    "key": "storage.local.diskSync.countTrigger",
    "value": 500,
    "defaultValue": 500,
    "overwritten": false,
    "protected": false,
    "validation": "min=0,max=1000000,required_if=Mode disk,required_if=Mode cache"
  },
  {
    "key": "storage.local.diskSync.intervalTrigger",
    "value": "50ms",
    "defaultValue": "50ms",
    "overwritten": false,
    "protected": false,
    "validation": "min=0,maxDuration=2s,required_if=Mode disk,required_if=Mode cache"
  },
  {
    "key": "storage.local.diskSync.mode",
    "value": "disk",
    "defaultValue": "disk",
    "overwritten": false,
    "protected": false,
    "validation": "required,oneof=disabled disk cache"
  },
  {
    "key": "storage.local.diskSync.wait",
    "value": true,
    "defaultValue": true,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.staging.maxSlicesPerFile",
    "value": 100,
    "defaultValue": 100,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=50000"
  },
  {
    "key": "storage.staging.upload.trigger.count",
    "value": 10000,
    "defaultValue": 10000,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=10000000"
  },
  {
    "key": "storage.staging.upload.trigger.interval",
    "value": "1m0s",
    "defaultValue": "1m0s",
    "overwritten": false,
    "protected": false,
    "validation": "required,minDuration=1s,maxDuration=30m"
  },
  {
    "key": "storage.staging.upload.trigger.size",
    "value": "1MB",
    "defaultValue": "1MB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=100B,maxBytes=50MB"
  },
  {
    "key": "storage.target.import.trigger.count",
    "value": 50000,
    "defaultValue": 50000,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=10000000"
  },
  {
    "key": "storage.target.import.trigger.interval",
    "value": "5m0s",
    "defaultValue": "5m0s",
    "overwritten": false,
    "protected": false,
    "validation": "required,minDuration=60s,maxDuration=24h"
  },
  {
    "key": "storage.target.import.trigger.size",
    "value": "5MB",
    "defaultValue": "5MB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=100B,maxBytes=500MB"
  },
  {
    "key": "storage.volumeAssignment.count",
    "value": 1,
    "defaultValue": 1,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=100"
  },
  {
    "key": "storage.volumeAssignment.preferredTypes",
    "value": [
      "default"
    ],
    "defaultValue": [
      "default"
    ],
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1"
  }
]
`), strings.TrimSpace(json.MustEncodeString(kvs, true)))
}

func TestConfig_BindKVs_Ok(t *testing.T) {
	t.Parallel()

	patch := tablesink.ConfigPatch{}
	require.NoError(t, configpatch.BindKVs(&patch, []configpatch.BindKV{
		{
			KeyPath: "storage.local.diskAllocation.size",
			Value:   "456MB",
		},
	}))

	assert.Equal(t, tablesink.ConfigPatch{
		Storage: &storage.ConfigPatch{
			Local: &local.ConfigPatch{
				DiskAllocation: &diskalloc.ConfigPatch{
					Size: test.Ptr(456 * datasize.MB),
				},
			},
		},
	}, patch)
}

func TestConfig_BindKVs_InvalidType(t *testing.T) {
	t.Parallel()

	err := configpatch.BindKVs(&tablesink.ConfigPatch{}, []configpatch.BindKV{
		{
			KeyPath: "storage.local.compression.gzip.level",
			Value:   "foo",
		},
	})

	if assert.Error(t, err) {
		assert.Equal(t, `invalid "storage.local.compression.gzip.level" value: found type "string", expected "int"`, err.Error())
	}
}

func TestConfig_BindKVs_InvalidValue(t *testing.T) {
	t.Parallel()

	err := configpatch.BindKVs(&tablesink.ConfigPatch{}, []configpatch.BindKV{
		{
			KeyPath: "storage.local.diskAllocation.size",
			Value:   "foo",
		},
	})

	if assert.Error(t, err) {
		assert.Equal(t, `invalid "storage.local.diskAllocation.size" value "foo": invalid syntax`, err.Error())
	}
}
