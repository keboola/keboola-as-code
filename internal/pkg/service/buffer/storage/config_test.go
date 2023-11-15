package storage

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/target"
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

	// Apply empty patch
	assert.Equal(t, defaultCfg, defaultCfg.With(ConfigPatch{}))

	// First patch
	expectedCfg := defaultCfg
	expectedCfg.Local.Compression = compressionCfg
	expectedCfg.Local.VolumesAssignment = volumesAssigmentCfg
	patchedConfig1 := defaultCfg.With(ConfigPatch{
		Local: local.ConfigPatch{
			Compression:       &compressionCfg,
			VolumesAssignment: &volumesAssigmentCfg,
		},
	})
	assert.Equal(t, expectedCfg, patchedConfig1)

	// Second patch
	expectedCfg.Local.DiskSync = diskSyncCfg
	expectedCfg.Local.DiskAllocation = diskAllocationCfg
	patchedConfig2 := patchedConfig1.With(ConfigPatch{
		Local: local.ConfigPatch{
			DiskSync:       &diskSyncCfg,
			DiskAllocation: &diskAllocationCfg,
		},
	})
	assert.Equal(t, expectedCfg, patchedConfig2)
}
