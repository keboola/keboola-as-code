package level_test

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	local "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	staging "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	target "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
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
				Count:          ptr.Ptr(2),
				PreferredTypes: ptr.Ptr([]string{"foo", "bar"}),
			},
		},
		Encoding: &encoding.ConfigPatch{
			Compression: &compression.ConfigPatch{
				GZIP: &compression.GZIPConfigPatch{
					Level:          ptr.Ptr(5),
					Implementation: ptr.Ptr(compression.GZIPImplFast),
					BlockSize:      ptr.Ptr(10 * datasize.MB),
					Concurrency:    ptr.Ptr(10),
				},
			},
		},
	}
	expectedCfg.Local.Encoding.Compression.GZIP = &compression.GZIPConfig{
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
		MaxSlicesPerFile: ptr.Ptr(1000),
		Upload: &staging.UploadConfigPatch{
			Trigger: &staging.UploadTriggerPatch{
				Count:    ptr.Ptr(uint64(30000)),
				Size:     ptr.Ptr(4 * datasize.MB),
				Interval: ptr.Ptr(duration.From(5 * time.Minute)),
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
	}, configpatch.WithModifyProtected()))
	assert.Equal(t, expectedCfg, patchedConfig2)

	// Second patch
	localConfigPatch2 := &local.ConfigPatch{
		Encoding: &encoding.ConfigPatch{
			Sync: &writesync.ConfigPatch{
				Mode:                     ptr.Ptr(writesync.ModeCache),
				Wait:                     ptr.Ptr(true),
				CheckInterval:            ptr.Ptr(duration.From(10 * time.Millisecond)),
				CountTrigger:             ptr.Ptr(uint(123)),
				UncompressedBytesTrigger: ptr.Ptr(1 * datasize.MB),
				CompressedBytesTrigger:   ptr.Ptr(1 * datasize.MB),
				IntervalTrigger:          ptr.Ptr(duration.From(100 * time.Millisecond)),
			},
		},
	}
	expectedCfg.Local.Encoding.Sync = writesync.Config{
		Mode:                     writesync.ModeCache,
		Wait:                     true,
		CheckInterval:            duration.From(10 * time.Millisecond),
		CountTrigger:             123,
		UncompressedBytesTrigger: 1 * datasize.MB,
		CompressedBytesTrigger:   1 * datasize.MB,
		IntervalTrigger:          duration.From(100 * time.Millisecond),
	}

	targetConfigPatch := &target.ConfigPatch{
		Import: &target.ImportConfigPatch{
			Trigger: &target.ImportTriggerPatch{
				Count:    ptr.Ptr(uint64(60000)),
				Size:     ptr.Ptr(7 * datasize.MB),
				Interval: ptr.Ptr(duration.From(8 * time.Minute)),
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
	}, configpatch.WithModifyProtected()))
	assert.Equal(t, expectedCfg, patchedConfig3)
}
