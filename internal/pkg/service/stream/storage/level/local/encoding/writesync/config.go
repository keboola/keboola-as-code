package writesync

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

const (
	// ModeDisk enables the synchronization of in-memory data to DISK.
	// Write operations wait for synchronization if Config.SyncWait = true.
	// Data will be preserved even in the event of a server power failure, but write takes more time.
	ModeDisk = Mode("disk")

	// ModeCache enables the synchronization of in-memory data to the OS DISK CACHE.
	// Write operations wait for synchronization if Config.SyncWait = true.
	// Data will be preserved in case of an unexpected process failure, but not in the event of a server power failure.
	// Synchronization from OS disk cache to disk is handled by the OS.
	ModeCache = Mode("cache")
)

type Mode string

// Config configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
//
// Synchronization is triggered by the conditions Config.BytesTrigger and Config.IntervalTrigger.
//
// Configuration matrix:
//   - When Mode=disk  and Wait=true,  writing will WAIT for synchronization to DISK.
//   - When Mode=disk  and Wait=false, writing will NOT WAIT for synchronization to DISK.
//   - When Mode=cache and Wait=true,  writing will WAIT for synchronization to OS DISK CACHE; synchronization to DISK is handled by the OS.
//   - When Mode=cache and Wait=false, writing will NOT WAIT for synchronization to OS DISK CACHE; synchronization to DISK is handled by the OS.
type Config struct {
	// Mode defines sync mode: more durable ModeDisk or faster ModeCache.
	Mode Mode `json:"mode" configKey:"mode" validate:"required,oneof=disk cache" configUsage:"Sync mode: \"cache\" or \"disk\"."`
	// Wait defines whether the operation should wait for sync.
	Wait bool `json:"wait" configKey:"wait" configUsage:"Wait for sync to disk OS cache or to disk hardware, depending on the mode." modAllowed:"true"`
	// CheckInterval defines how often BytesTrigger and IntervalTrigger will be checked.
	// It is minimal interval between two syncs.
	CheckInterval duration.Duration `json:"checkInterval,omitempty" configKey:"checkInterval" validate:"required,minDuration=1ms,maxDuration=30s" configUsage:"Minimal interval between syncs to disk."`
	// CountTrigger defines the writes count after the sync will be triggered.
	// The number is count of the high-level writers, e.g., one table row = one write operation.
	CountTrigger uint `json:"countTrigger" configKey:"countTrigger,omitempty" validate:"required,min=1,max=1000000" configUsage:"Written records count to trigger sync."`
	// UncompressedBytesTrigger defines the size after the sync will be triggered.
	// Bytes are measured at the start of the writers Pipeline.
	UncompressedBytesTrigger datasize.ByteSize `json:"uncompressedBytesTrigger,omitempty" configKey:"uncompressedBytesTrigger" validate:"required,minBytes=100B,maxBytes=500MB" configUsage:"Size of buffered uncompressed data to trigger sync."`
	// CompressedBytesTrigger defines the size after the sync will be triggered.
	// Bytes are measured at the end of the writers Pipeline.
	CompressedBytesTrigger datasize.ByteSize `json:"compressedBytesTrigger,omitempty" configKey:"compressedBytesTrigger" validate:"required,minBytes=100B,maxBytes=100MB" configUsage:"Size of buffered compressed data to trigger sync."`
	// IntervalTrigger defines the interval from the last sync after the sync will be triggered.
	IntervalTrigger duration.Duration `json:"intervalTrigger,omitempty" configKey:"intervalTrigger" validate:"required,minDuration=10ms,maxDuration=30s" configUsage:"Interval from the last sync to trigger sync."`
	// OverrideSyncerFactory overrides the DefaultSyncerFactory.
	// A custom implementation can be useful for tests.
	OverrideSyncerFactory SyncerFactory `json:"-"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Mode                     *Mode              `json:"mode,omitempty"`
	Wait                     *bool              `json:"wait,omitempty"`
	CheckInterval            *duration.Duration `json:"checkInterval,omitempty"`
	CountTrigger             *uint              `json:"countTrigger,omitempty"`
	UncompressedBytesTrigger *datasize.ByteSize `json:"uncompressedBytesTrigger,omitempty"`
	CompressedBytesTrigger   *datasize.ByteSize `json:"compressedBytesTrigger,omitempty"`
	IntervalTrigger          *duration.Duration `json:"intervalTrigger,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Mode:                     ModeDisk,
		Wait:                     true,
		CheckInterval:            duration.From(5 * time.Millisecond),
		CountTrigger:             10000,
		UncompressedBytesTrigger: 1 * datasize.MB,
		CompressedBytesTrigger:   256 * datasize.KB,
		IntervalTrigger:          duration.From(50 * time.Millisecond),
	}
}
