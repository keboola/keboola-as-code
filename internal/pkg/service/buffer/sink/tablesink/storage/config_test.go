package storage

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

func TestConfig_With(t *testing.T) {
	t.Parallel()

	defaultConfig := Config{
		Local:   local.NewConfig(),
		Staging: staging.NewConfig(),
		Target:  target.NewConfig(),
	}

	// Apply empty patch
	patchedConfig1 := defaultConfig
	require.NoError(t, configpatch.Apply(&patchedConfig1, ConfigPatch{}))
	assert.Equal(t, defaultConfig, patchedConfig1)

	// First patch
	expectedCfg := defaultConfig
	localConfigPatch := &local.ConfigPatch{
		Compression: &compression.ConfigPatch{
			GZIP: &compression.GZIPConfigPatch{
				Level:          ptr(5),
				Implementation: ptr(compression.GZIPImplFast),
				BlockSize:      ptr(10 * datasize.MB),
				Concurrency:    ptr(10),
			},
		},
	}
	expectedCfg.Local.Compression.GZIP = &compression.GZIPConfig{
		Level:          5,
		Implementation: compression.GZIPImplFast,
		BlockSize:      10 * datasize.MB,
		Concurrency:    10,
	}
	volumeAssignmentPatch := &assignment.ConfigPatch{
		Count:          ptr(2),
		PreferredTypes: ptr([]string{"foo", "bar"}),
	}
	expectedCfg.VolumeAssignment = assignment.Config{
		Count:          2,
		PreferredTypes: []string{"foo", "bar"},
	}
	stagingConfigPatch := &staging.ConfigPatch{
		MaxSlicesPerFile: ptr(1000),
		Upload: &staging.UploadConfigPatch{
			Trigger: &staging.UploadTriggerPatch{
				Count:    ptr(uint64(30000)),
				Size:     ptr(4 * datasize.MB),
				Interval: ptr(duration.From(5 * time.Minute)),
			},
		},
	}
	expectedCfg.Staging.MaxSlicesPerFile = 1000
	expectedCfg.Staging.Upload.Trigger = staging.UploadTrigger{
		Count:    30000,
		Size:     4 * datasize.MB,
		Interval: duration.From(5 * time.Minute),
	}
	// Compare
	patchedConfig2 := patchedConfig1
	require.NoError(t, configpatch.Apply(&patchedConfig2, ConfigPatch{
		Local:            localConfigPatch,
		Staging:          stagingConfigPatch,
		VolumeAssignment: volumeAssignmentPatch,
	}))
	assert.Equal(t, expectedCfg, patchedConfig2)

	// Second patch
	localConfigPatch2 := &local.ConfigPatch{
		DiskSync: &disksync.ConfigPatch{
			Mode:            ptr(disksync.ModeCache),
			Wait:            ptr(true),
			CheckInterval:   ptr(duration.From(10 * time.Millisecond)),
			CountTrigger:    ptr(uint(123)),
			BytesTrigger:    ptr(1 * datasize.MB),
			IntervalTrigger: ptr(duration.From(100 * time.Millisecond)),
		},
		DiskAllocation: &diskalloc.ConfigPatch{
			Enabled:     ptr(true),
			Size:        ptr(10 * datasize.MB),
			SizePercent: ptr(150),
		},
	}
	expectedCfg.Local.DiskSync = disksync.Config{
		Mode:            disksync.ModeCache,
		Wait:            true,
		CheckInterval:   duration.From(10 * time.Millisecond),
		CountTrigger:    123,
		BytesTrigger:    1 * datasize.MB,
		IntervalTrigger: duration.From(100 * time.Millisecond),
	}

	expectedCfg.Local.DiskAllocation = diskalloc.Config{
		Enabled:     true,
		Size:        10 * datasize.MB,
		SizePercent: 150,
	}
	targetConfigPatch := &target.ConfigPatch{
		Import: &target.ImportConfigPatch{
			Trigger: &target.ImportTriggerPatch{
				Count:    ptr(uint64(60000)),
				Size:     ptr(7 * datasize.MB),
				Interval: ptr(duration.From(8 * time.Minute)),
			},
		},
	}
	expectedCfg.Target.Import.Trigger = target.ImportTrigger{
		Count:    60000,
		Size:     7 * datasize.MB,
		Interval: duration.From(8 * time.Minute),
	}
	// Compare
	patchedConfig3 := patchedConfig2
	require.NoError(t, configpatch.Apply(&patchedConfig3, ConfigPatch{
		Local:  localConfigPatch2,
		Target: targetConfigPatch,
	}))
	assert.Equal(t, expectedCfg, patchedConfig3)
}
