package testconfig

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	local "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/config"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
)

func StorageConfigPatch() configpatch.PatchKVs {
	return configpatch.MustDumpPatch(
		config.Config{},
		config.Patch{
			Storage: &storage.ConfigPatch{
				Level: &level.ConfigPatch{
					Local: &local.ConfigPatch{
						Encoding: &encoding.ConfigPatch{
							Sync: &writesync.ConfigPatch{
								Mode:                     new(writesync.ModeDisk),
								Wait:                     new(false),
								CheckInterval:            new(duration.From(1 * time.Millisecond)),
								CountTrigger:             new(uint(100)),
								UncompressedBytesTrigger: new(200 * datasize.KB),
								CompressedBytesTrigger:   new(100 * datasize.KB),
								IntervalTrigger:          new(duration.From(100 * time.Millisecond)),
							},
						},
						Volume: &volume.ConfigPatch{
							Assignment: &assignment.ConfigPatch{
								Count:          new(1),
								PreferredTypes: new([]string{"default"}),
							},
						},
					},
				},
			},
		},
		configpatch.WithModifyProtected(),
	)
}

func LocalVolumeConfig(count int, preferred []string) configpatch.PatchKVs {
	return configpatch.MustDumpPatch(
		config.Config{},
		config.Patch{
			Storage: &storage.ConfigPatch{
				Level: &level.ConfigPatch{
					Local: &local.ConfigPatch{
						Volume: &volume.ConfigPatch{
							Assignment: &assignment.ConfigPatch{
								Count:          new(count),
								PreferredTypes: new(preferred),
							},
						},
					},
				},
			},
		},
		configpatch.WithModifyProtected(),
	)
}
