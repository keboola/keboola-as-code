package slicestate

import (
	"context"

	"github.com/qmuntal/stateless"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Opened
// It is the initial state of the slice.
// API nodes can write records to the related etcd prefix.
const Opened State = "opened"

// Closing
// Upload conditions have been met.
// Waiting for the API nodes until they switch to the new slice.
const Closing State = "closing"

// Uploading
// The slice is ready for upload.
// Some worker is/will be uploading it.
const Uploading State = "uploading"

// Uploaded
// The slice has been successfully uploaded.
const Uploaded State = "uploaded"

// Failed
// Upload failed, try again later.
const Failed State = "failed"

type State string

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
	v.permit(Opened, Closing)
	v.permit(Closing, Uploading)
	v.permit(Uploading, Failed)
	v.permit(Failed, Uploading)
	v.permit(Uploading, Uploaded)
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
