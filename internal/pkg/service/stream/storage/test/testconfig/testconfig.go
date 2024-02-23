package testconfig

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
)

func StorageConfigPatch() configpatch.PatchKVs {
	return configpatch.MustDumpPatch(
		config.Config{},
		config.Patch{
			Storage: &storage.ConfigPatch{
				Level: &level.ConfigPatch{
					Local: &local.ConfigPatch{
						Volume: &volume.ConfigPatch{
							Assignment: &assignment.ConfigPatch{
								Count:          ptr(1),
								PreferredTypes: ptr([]string{"default"}),
							},
							Sync: &disksync.ConfigPatch{
								Mode:            ptr(disksync.ModeDisk),
								Wait:            ptr(false),
								CheckInterval:   ptr(duration.From(1 * time.Millisecond)),
								CountTrigger:    ptr(uint(100)),
								BytesTrigger:    ptr(100 * datasize.KB),
								IntervalTrigger: ptr(duration.From(100 * time.Millisecond)),
							},
						},
					},
				},
			},
		})
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
								Count:          ptr(count),
								PreferredTypes: ptr(preferred),
							},
						},
					},
				},
			},
		},
	)
}

func ptr[T any](v T) *T {
	return &v
}
