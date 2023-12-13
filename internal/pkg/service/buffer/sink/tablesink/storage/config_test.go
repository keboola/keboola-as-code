package storage

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConfig_With(t *testing.T) {
	t.Parallel()

	defaultCfg := Config{
		Local:   local.NewConfig(),
		Staging: staging.NewConfig(),
		Target:  target.NewConfig(),
	}

	// Fixtures
	compressionCfg := compression.Config{
		Type: compression.TypeGZIP,
		GZIP: &compression.GZIPConfig{
			Level:          5,
			Implementation: compression.GZIPImplFast,
			BlockSize:      10 * datasize.MB,
			Concurrency:    10,
		},
	}
	volumesAssigmentCfg := local.VolumesAssignment{
		PerPod:         2,
		PreferredTypes: []string{"foo", "bar"},
	}
	diskSyncCfg := disksync.Config{
		Mode:            disksync.ModeCache,
		Wait:            true,
		CheckInterval:   10 * time.Millisecond,
		CountTrigger:    123,
		BytesTrigger:    1 * datasize.MB,
		IntervalTrigger: 100 * time.Millisecond,
	}
	diskAllocationCfg := local.DiskAllocation{
		Enabled:     true,
		Size:        10 * datasize.MB,
		SizePercent: 150,
	}
	maxSlicesPerFile := 1000
	sliceUploadTrigger := staging.SliceUploadTrigger{
		Count:    30000,
		Size:     4 * datasize.MB,
		Interval: 5 * time.Minute,
	}
	fileImportTrigger := target.FileImportTrigger{
		Count:    60000,
		Size:     7 * datasize.MB,
		Interval: 8 * time.Minute,
	}

	// Apply nil patch
	assert.Equal(t, defaultCfg, defaultCfg.With(nil))

	// Apply empty patch
	assert.Equal(t, defaultCfg, defaultCfg.With(&ConfigPatch{}))

	// First patch
	expectedCfg := defaultCfg
	expectedCfg.Local.Compression = compressionCfg
	expectedCfg.Local.VolumesAssignment = volumesAssigmentCfg
	expectedCfg.Staging.MaxSlicesPerFile = maxSlicesPerFile
	expectedCfg.Staging.Upload.Trigger = sliceUploadTrigger
	patchedConfig1 := defaultCfg.With(&ConfigPatch{
		Local: &local.ConfigPatch{
			Compression:       &compressionCfg,
			VolumesAssignment: &volumesAssigmentCfg,
		},
		Staging: &staging.ConfigPatch{
			MaxSlicesPerFile: &maxSlicesPerFile,
			Upload: &staging.UploadConfigPatch{
				Trigger: &sliceUploadTrigger,
			},
		},
	})
	assert.Equal(t, expectedCfg, patchedConfig1)

	// Second patch
	expectedCfg.Local.DiskSync = diskSyncCfg
	expectedCfg.Local.DiskAllocation = diskAllocationCfg
	expectedCfg.Target.Import.Trigger = fileImportTrigger
	patchedConfig2 := patchedConfig1.With(&ConfigPatch{
		Local: &local.ConfigPatch{
			DiskSync:       &diskSyncCfg,
			DiskAllocation: &diskAllocationCfg,
		},
		Target: &target.ConfigPatch{
			Import: &target.ImportConfigPatch{
				Trigger: &fileImportTrigger,
			},
		},
	})
	assert.Equal(t, expectedCfg, patchedConfig2)
}
