// Package slicestate provides transitions between allowed slice states.
//
// ## States
//
//	Writing:   active/opened/writing/          - API nodes are writing new records to the slice
//	Closing:   active/opened/closing/          - We are waiting for the API nodes to stop writing to the slice.
//	Uploading: active/closed/uploading/        - Slice upload is in progress.
//	Uploaded:  active/closed/failed/           - Slice upload succeed, the parent file will be imported.
//	Failed:    active/closed/uploaded/         - Slice upload failed, it will be retried.
//	Imported:  archived/successful/imported/   - Slice has been imported to a target table.
//
// ## State Groups
//
// They are used to watch a group of states.
//
//	AllActive:       active/                   - All slices that have not yet been imported.
//	  AllOpened      active/opened/            - All slices to which records are written.
//	  AllClosed      active/closed/            - All slices waiting for import (and some also for upload).
//	AllArchived:     archived/                 - All the slices we don't have to care about anymore.
//	  AllSuccessful: archived/successful/      - All slices completed successfully.
package slicestate

import (
	"context"
	"strings"

	"github.com/qmuntal/stateless"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Writing
// It is the initial state of the slice.
// API nodes can write records to the related etcd prefix.
const Writing State = "active/opened/writing"

// Closing
// Upload conditions have been met.
// Waiting for the API nodes until they switch to the new slice.
const Closing State = "active/opened/closing"

// Uploading
// The slice is ready for upload.
// Some worker is/will be uploading it.
const Uploading State = "active/closed/uploading"

// Uploaded
// The slice has been successfully uploaded.
const Uploaded State = "active/closed/uploaded"

// Failed
// Upload failed, try again later.
const Failed State = "active/closed/failed"

// Imported
// The parent File has been successfully imported to the target table.
const Imported State = "archived/successful/imported"

// State groups, see package documentation.
const (
	AllActive     StateGroup = "active"
	AllArchived   StateGroup = "archived"
	AllOpened     StateGroup = "active/opened"
	AllClosed     StateGroup = "active/closed"
	AllSuccessful StateGroup = "archived/successful"
)

type State string

type StateGroup string

type onEntry func(ctx context.Context, from, to State) error

type STM struct {
	onEntry onEntry
	stm     *stateless.StateMachine
}

func NewSTM(state State, fn onEntry) *STM {
	v := &STM{onEntry: fn, stm: stateless.NewStateMachine(state)}
	v.stm.OnUnhandledTrigger(func(_ context.Context, state stateless.State, trigger stateless.Trigger, _ []string) error {
		return errors.Errorf(`slice state transition "%s" -> "%s" is not allowed`, state, trigger)
	})
	v.permit(Writing, Closing)
	v.permit(Closing, Uploading)
	v.permit(Uploading, Failed)
	v.permit(Failed, Uploading)
	v.permit(Uploading, Uploaded)
	v.permit(Uploaded, Imported)
	return v
}

func (v *STM) To(ctx context.Context, to State) error {
	return v.stm.FireCtx(ctx, to)
}

func (v *STM) permit(from, to State) {
	v.stm.
		Configure(from).
		Permit(to, to). // first argument, trigger = to, see To method
		OnExit(func(ctx context.Context, args ...any) error {
			if stateless.GetTransition(ctx).Destination == to {
				return v.onEntry(ctx, from, to)
			}
			return nil
		})
}

func (v State) String() string {
	return string(v)
}

func (v State) StateShort() string {
	pfx := v.String()
	return pfx[strings.LastIndex(pfx, "/")+1:]
}

func (v StateGroup) String() string {
	return string(v)
}

func All() []State {
	return []State{
		Writing,
		Closing,
		Uploading,
		Uploaded,
		Failed,
		Imported,
	}
}
