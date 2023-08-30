package disksync

import (
	"github.com/c2h5oh/datasize"
	"time"
)

const (
	// ModeDisabled disables the synchronization of in-memory data to DISK or OS DISK CACHE.
	// Synchronization to OS DISK CACHE is only done when the memory buffers in the process are full.
	// Synchronization to DISK is enforced only when closing a slice, otherwise it is handled by the OS.
	ModeDisabled = "disabled"

	// ModeDisk enables the synchronization of in-memory data to DISK.
	// Write operations wait for synchronization if Config.SyncWait = true.
	// Data will be preserved even in the event of a server power failure, but write takes more time.
	ModeDisk = "disk"

	// ModeCache enables the synchronization of in-memory data to the OS DISK CACHE.
	// Write operations wait for synchronization if Config.SyncWait = true.
	// Data will be preserved in case of an unexpected process failure, but not in the event of a server power failure.
	// Synchronization from OS disk cache to disk is handled by the OS.
	ModeCache = "cache"
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
	Mode Mode `json:"mode" validate:"required,oneof=disabled disk cache"`
	// Wait defines whether the operation should wait for sync.
	Wait bool `json:"wait" validate:"excluded_if= Mode disabled"`
	// BytesTrigger defines the size after the sync will be triggered.
	BytesTrigger datasize.ByteSize `json:"bytesTrigger,omitempty" validate:"maxBytes=100MB,excluded_if=Mode disabled,required_if=Mode disk,required_if=Mode cache"`
	// IntervalTrigger defines the interval after the sync will be triggered.
	IntervalTrigger time.Duration `json:"intervalTrigger,omitempty"  validate:"min=0,maxDuration=2s,excluded_if=Mode disabled,required_if=Mode disk,required_if=Mode cache"`
}

func DefaultConfig() Config {
	return Config{
		Mode:            ModeDisk,
		Wait:            true,
		BytesTrigger:    128 * datasize.KB,
		IntervalTrigger: 100 * time.Millisecond,
	}
}
