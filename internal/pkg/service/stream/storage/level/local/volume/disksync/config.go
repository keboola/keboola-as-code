package disksync

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

const (
	// ModeDisabled disables the synchronization of in-memory data to DISK or OS DISK CACHE.
	// Synchronization to OS DISK CACHE is only done when the memory buffers in the process are full.
	// Synchronization to DISK is enforced only when closing a slice, otherwise it is handled by the OS.
	ModeDisabled = Mode("disabled")

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
	Mode Mode `json:"mode" configKey:"mode" validate:"required,oneof=disabled disk cache" configUsage:"Sync mode: \"disabled\", \"cache\" or \"disk\"."`
	// Wait defines whether the operation should wait for sync.
	Wait bool `json:"wait" configKey:"wait" configUsage:"Wait for sync to disk OS cache or to disk hardware, depending on the mode."`
	// CheckInterval defines how often BytesTrigger and IntervalTrigger will be checked.
	// It is minimal interval between two syncs.
	CheckInterval duration.Duration `json:"checkInterval,omitempty" configKey:"checkInterval" validate:"min=0,maxDuration=2s,required_if=Mode disk,required_if=Mode cache" configUsage:"Minimal interval between syncs to disk."`
	// CountTrigger defines the writes count after the sync will be triggered.
	// The number is count of the high-level writers, e.g., one table row = one write operation.
	CountTrigger uint `json:"countTrigger,omitempty" configKey:"countTrigger" validate:"min=0,max=1000000,required_if=Mode disk,required_if=Mode cache" configUsage:"Written records count to trigger sync."`
	// BytesTrigger defines the size after the sync will be triggered.
	// Bytes are measured at the beginning of the writers chain.
	BytesTrigger datasize.ByteSize `json:"bytesTrigger,omitempty" configKey:"bytesTrigger" validate:"maxBytes=100MB,required_if=Mode disk,required_if=Mode cache" configUsage:"Written size to trigger sync."`
	// IntervalTrigger defines the interval from the last sync after the sync will be triggered.
	IntervalTrigger duration.Duration `json:"intervalTrigger,omitempty" configKey:"intervalTrigger" validate:"min=0,maxDuration=2s,required_if=Mode disk,required_if=Mode cache" configUsage:"Interval from the last sync to trigger sync."`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Mode            *Mode              `json:"mode,omitempty"`
	Wait            *bool              `json:"wait,omitempty"`
	CheckInterval   *duration.Duration `json:"checkInterval,omitempty"`
	CountTrigger    *uint              `json:"countTrigger,omitempty"`
	BytesTrigger    *datasize.ByteSize `json:"bytesTrigger,omitempty"`
	IntervalTrigger *duration.Duration `json:"intervalTrigger,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Mode:            ModeDisk,
		Wait:            true,
		CheckInterval:   duration.From(5 * time.Millisecond),
		CountTrigger:    500,
		BytesTrigger:    1 * datasize.MB,
		IntervalTrigger: duration.From(50 * time.Millisecond),
	}
}
