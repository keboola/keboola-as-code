package tablesink_test

import (
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestConfig_ToKVs(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpAll(
		tablesink.NewRuntimeConfig(storage.NewConfig()),
		tablesink.ConfigPatch{
			Storage: &storage.ConfigPatch{
				Level: &level.ConfigPatch{
					Local: &local.ConfigPatch{
						Volume: &volume.ConfigPatch{
							Allocation: &diskalloc.ConfigPatch{
								Static: test.Ptr(456 * datasize.MB),
							},
						},
					},
				},
			},
		},
	)
	require.NoError(t, err)

	assert.Equal(t, strings.TrimSpace(`
[
  {
    "key": "storage.level.local.compression.gzip.blockSize",
    "value": "256KB",
    "defaultValue": "256KB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=16kB,maxBytes=100MB"
  },
  {
    "key": "storage.level.local.compression.gzip.concurrency",
    "value": 0,
    "defaultValue": 0,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.level.local.compression.gzip.implementation",
    "value": "parallel",
    "defaultValue": "parallel",
    "overwritten": false,
    "protected": false,
    "validation": "required,oneof=standard fast parallel"
  },
  {
    "key": "storage.level.local.compression.gzip.level",
    "value": 1,
    "defaultValue": 1,
    "overwritten": false,
    "protected": false,
    "validation": "min=1,max=9"
  },
  {
    "key": "storage.level.local.compression.type",
    "value": "gzip",
    "defaultValue": "gzip",
    "overwritten": false,
    "protected": false,
    "validation": "required,oneof=none gzip zstd"
  },
  {
    "key": "storage.level.local.compression.zstd.concurrency",
    "value": 0,
    "defaultValue": 0,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.level.local.compression.zstd.level",
    "value": 1,
    "defaultValue": 1,
    "overwritten": false,
    "protected": false,
    "validation": "min=1,max=4"
  },
  {
    "key": "storage.level.local.compression.zstd.windowSize",
    "value": "1MB",
    "defaultValue": "1MB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=1kB,maxBytes=512MB"
  },
  {
    "key": "storage.level.local.volume.allocation.enabled",
    "value": true,
    "defaultValue": true,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.level.local.volume.allocation.relative",
    "value": 110,
    "defaultValue": 110,
    "overwritten": false,
    "protected": false,
    "validation": "min=100,max=500"
  },
  {
    "key": "storage.level.local.volume.allocation.static",
    "value": "456MB",
    "defaultValue": "100MB",
    "overwritten": true,
    "protected": false,
    "validation": "required"
  },
  {
    "key": "storage.level.local.volume.assignment.count",
    "value": 1,
    "defaultValue": 1,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=100"
  },
  {
    "key": "storage.level.local.volume.assignment.preferredTypes",
    "value": [
      "default"
    ],
    "defaultValue": [
      "default"
    ],
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1"
  },
  {
    "key": "storage.level.local.volume.sync.bytesTrigger",
    "value": "1MB",
    "defaultValue": "1MB",
    "overwritten": false,
    "protected": false,
    "validation": "maxBytes=100MB,required_if=Mode disk,required_if=Mode cache"
  },
  {
    "key": "storage.level.local.volume.sync.checkInterval",
    "value": "5ms",
    "defaultValue": "5ms",
    "overwritten": false,
    "protected": false,
    "validation": "min=0,maxDuration=2s,required_if=Mode disk,required_if=Mode cache"
  },
  {
    "key": "storage.level.local.volume.sync.countTrigger",
    "value": 500,
    "defaultValue": 500,
    "overwritten": false,
    "protected": false,
    "validation": "min=0,max=1000000,required_if=Mode disk,required_if=Mode cache"
  },
  {
    "key": "storage.level.local.volume.sync.intervalTrigger",
    "value": "50ms",
    "defaultValue": "50ms",
    "overwritten": false,
    "protected": false,
    "validation": "min=0,maxDuration=2s,required_if=Mode disk,required_if=Mode cache"
  },
  {
    "key": "storage.level.local.volume.sync.mode",
    "value": "disk",
    "defaultValue": "disk",
    "overwritten": false,
    "protected": false,
    "validation": "required,oneof=disabled disk cache"
  },
  {
    "key": "storage.level.local.volume.sync.wait",
    "value": true,
    "defaultValue": true,
    "overwritten": false,
    "protected": false
  },
  {
    "key": "storage.level.staging.maxSlicesPerFile",
    "value": 100,
    "defaultValue": 100,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=50000"
  },
  {
    "key": "storage.level.staging.upload.trigger.count",
    "value": 10000,
    "defaultValue": 10000,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=10000000"
  },
  {
    "key": "storage.level.staging.upload.trigger.interval",
    "value": "1m0s",
    "defaultValue": "1m0s",
    "overwritten": false,
    "protected": false,
    "validation": "required,minDuration=1s,maxDuration=30m"
  },
  {
    "key": "storage.level.staging.upload.trigger.size",
    "value": "1MB",
    "defaultValue": "1MB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=100B,maxBytes=50MB"
  },
  {
    "key": "storage.level.target.import.trigger.count",
    "value": 50000,
    "defaultValue": 50000,
    "overwritten": false,
    "protected": false,
    "validation": "required,min=1,max=10000000"
  },
  {
    "key": "storage.level.target.import.trigger.interval",
    "value": "5m0s",
    "defaultValue": "5m0s",
    "overwritten": false,
    "protected": false,
    "validation": "required,minDuration=60s,maxDuration=24h"
  },
  {
    "key": "storage.level.target.import.trigger.size",
    "value": "5MB",
    "defaultValue": "5MB",
    "overwritten": false,
    "protected": false,
    "validation": "required,minBytes=100B,maxBytes=500MB"
  }
]
`), strings.TrimSpace(json.MustEncodeString(kvs, true)))
}

func TestConfig_BindKVs_Ok(t *testing.T) {
	t.Parallel()

	patch := tablesink.ConfigPatch{}
	require.NoError(t, configpatch.BindKVs(&patch, []configpatch.PatchKV{
		{
			KeyPath: "storage.level.local.volume.allocation.static",
			Value:   "456MB",
		},
	}))

	assert.Equal(t, tablesink.ConfigPatch{
		Storage: &storage.ConfigPatch{
			Level: &level.ConfigPatch{
				Local: &local.ConfigPatch{
					Volume: &volume.ConfigPatch{
						Allocation: &diskalloc.ConfigPatch{
							Static: test.Ptr(456 * datasize.MB),
						},
					},
				},
			},
		},
	}, patch)
}

func TestConfig_BindKVs_InvalidType(t *testing.T) {
	t.Parallel()

	err := configpatch.BindKVs(&tablesink.ConfigPatch{}, []configpatch.PatchKV{
		{
			KeyPath: "storage.level.local.compression.gzip.level",
			Value:   "foo",
		},
	})

	if assert.Error(t, err) {
		assert.Equal(t, `invalid "storage.level.local.compression.gzip.level" value: found type "string", expected "int"`, err.Error())
	}
}

func TestConfig_BindKVs_InvalidValue(t *testing.T) {
	t.Parallel()

	err := configpatch.BindKVs(&tablesink.ConfigPatch{}, []configpatch.PatchKV{
		{
			KeyPath: "storage.level.local.volume.allocation.static",
			Value:   "foo",
		},
	})

	if assert.Error(t, err) {
		assert.Equal(t, `invalid "storage.level.local.volume.allocation.static" value "foo": invalid syntax`, err.Error())
	}
}
