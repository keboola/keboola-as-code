// Package filestate provides transitions between allowed file states.
package filestate

import (
	"context"

	"github.com/qmuntal/stateless"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Opened
// It is the initial state of the file.
// API nodes can write records to an open slice.
const Opened State = "opened"

// Closing
// Import conditions have been met.
// Waiting for the last slice to be closed.
const Closing State = "closing"

// Importing
// File import into the table is in progress.
const Importing State = "importing"

// Imported
// The file has been successfully imported.
const Imported State = "imported"

// Failed
// Import failed, try again later.
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
		return errors.Errorf(`file state transition "%s" -> "%s" is not allowed`, state, trigger)
	})
	v.permit(Opened, Closing)
	v.permit(Closing, Importing)
	v.permit(Importing, Failed)
	v.permit(Failed, Importing)
	v.permit(Importing, Imported)
	return v
}

// To triggers state transition.
func (v *STM) To(ctx context.Context, to State) error {
	return v.stm.FireCtx(ctx, to)
}

// permit registers a new transition.
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

func All() []State {
	return []State{Opened, Closing, Importing, Imported, Failed}
}
