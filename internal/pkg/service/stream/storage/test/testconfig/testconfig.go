package testconfig

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/source/writesync"
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
						Volume: &volume.ConfigPatch{
							Assignment: &assignment.ConfigPatch{
								Count:          ptr.Ptr(1),
								PreferredTypes: ptr.Ptr([]string{"default"}),
							},
							Sync: &writesync.ConfigPatch{
								Mode:                     ptr.Ptr(writesync.ModeDisk),
								Wait:                     ptr.Ptr(false),
								CheckInterval:            ptr.Ptr(duration.From(1 * time.Millisecond)),
								CountTrigger:             ptr.Ptr(uint(100)),
								UncompressedBytesTrigger: ptr.Ptr(200 * datasize.KB),
								CompressedBytesTrigger:   ptr.Ptr(100 * datasize.KB),
								IntervalTrigger:          ptr.Ptr(duration.From(100 * time.Millisecond)),
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
								Count:          ptr.Ptr(count),
								PreferredTypes: ptr.Ptr(preferred),
							},
						},
					},
				},
			},
		},
		configpatch.WithModifyProtected(),
	)
}
