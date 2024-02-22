package level_test

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestConfig_With(t *testing.T) {
	t.Parallel()

	defaultConfig := level.NewConfig()

	// Apply empty patch
	patchedConfig1 := defaultConfig
	require.NoError(t, configpatch.Apply(&patchedConfig1, level.ConfigPatch{}))
	assert.Equal(t, defaultConfig, patchedConfig1)

	// First patch
	expectedCfg := defaultConfig
	localConfigPatch := &local.ConfigPatch{
		Volume: &volume.ConfigPatch{
			Assignment: &assignment.ConfigPatch{
				Count:          test.Ptr(2),
				PreferredTypes: test.Ptr([]string{"foo", "bar"}),
			},
		},
		Compression: &compression.ConfigPatch{
			GZIP: &compression.GZIPConfigPatch{
				Level:          test.Ptr(5),
				Implementation: test.Ptr(compression.GZIPImplFast),
				BlockSize:      test.Ptr(10 * datasize.MB),
				Concurrency:    test.Ptr(10),
			},
		},
	}
	expectedCfg.Local.Compression.GZIP = &compression.GZIPConfig{
		Level:          5,
		Implementation: compression.GZIPImplFast,
		BlockSize:      10 * datasize.MB,
		Concurrency:    10,
	}
	expectedCfg.Local.Volume.Assignment = assignment.Config{
		Count:          2,
		PreferredTypes: []string{"foo", "bar"},
	}
	stagingConfigPatch := &staging.ConfigPatch{
		MaxSlicesPerFile: test.Ptr(1000),
		Upload: &staging.UploadConfigPatch{
			Trigger: &staging.UploadTriggerPatch{
				Count:    test.Ptr(uint64(30000)),
				Size:     test.Ptr(4 * datasize.MB),
				Interval: test.Ptr(duration.From(5 * time.Minute)),
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
	require.NoError(t, configpatch.Apply(&patchedConfig2, level.ConfigPatch{
		Local:   localConfigPatch,
		Staging: stagingConfigPatch,
	}))
	assert.Equal(t, expectedCfg, patchedConfig2)

	// Second patch
	localConfigPatch2 := &local.ConfigPatch{
		Volume: &volume.ConfigPatch{
			Sync: &disksync.ConfigPatch{
				Mode:            test.Ptr(disksync.ModeCache),
				Wait:            test.Ptr(true),
				CheckInterval:   test.Ptr(duration.From(10 * time.Millisecond)),
				CountTrigger:    test.Ptr(uint(123)),
				BytesTrigger:    test.Ptr(1 * datasize.MB),
				IntervalTrigger: test.Ptr(duration.From(100 * time.Millisecond)),
			},
			Allocation: &diskalloc.ConfigPatch{
				Enabled:  test.Ptr(true),
				Static:   test.Ptr(10 * datasize.MB),
				Relative: test.Ptr(150),
			},
		},
	}
	expectedCfg.Local.Volume.Sync = disksync.Config{
		Mode:            disksync.ModeCache,
		Wait:            true,
		CheckInterval:   duration.From(10 * time.Millisecond),
		CountTrigger:    123,
		BytesTrigger:    1 * datasize.MB,
		IntervalTrigger: duration.From(100 * time.Millisecond),
	}

	expectedCfg.Local.Volume.Allocation = diskalloc.Config{
		Enabled:  true,
		Static:   10 * datasize.MB,
		Relative: 150,
	}
	targetConfigPatch := &target.ConfigPatch{
		Import: &target.ImportConfigPatch{
			Trigger: &target.ImportTriggerPatch{
				Count:    test.Ptr(uint64(60000)),
				Size:     test.Ptr(7 * datasize.MB),
				Interval: test.Ptr(duration.From(8 * time.Minute)),
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
	require.NoError(t, configpatch.Apply(&patchedConfig3, level.ConfigPatch{
		Local:  localConfigPatch2,
		Target: targetConfigPatch,
	}))
	assert.Equal(t, expectedCfg, patchedConfig3)
}
