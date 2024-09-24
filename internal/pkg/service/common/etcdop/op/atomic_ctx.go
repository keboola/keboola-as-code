package op

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	actualAtomicOpCtxKey  = ctxKey("actualAtomicOp")
	atomicOpMaxReadLevels = 10
)

type ctxKey string

type atomicOpCore = AtomicOpCore

// AtomicOpCtx - aux struct, part of the context, it provides actual atomic operation to be extended.
type AtomicOpCtx struct {
	*atomicOpCore
	*atomicOpStore
	closed bool
}

type atomicOpStore struct {
	store map[string]any
}

// AtomicOpCtxFrom gets actual atomic operation from the context.
//
// It can be used to add some additional READ operations based on result from a previous READ operation via *AtomicOpCore methods.
// See AtomicOp.Do method and TestAtomicFromCtx_Complex for details.
//
// In addition, it is possible to set auxiliary key/value pairs that should be available in the atomic operation.
// See atomicOpStore struct.
func AtomicOpCtxFrom(ctx context.Context) *AtomicOpCtx {
	actualOp, ok := ctx.Value(actualAtomicOpCtxKey).(*AtomicOpCtx)
	if !ok {
		panic(errors.New("no atomic operation found in the context"))
	}
	if actualOp.closed {
		panic(errors.New("atomic operation in the context is closed"))
	}
	return actualOp
}

func newAtomicOpCtx(core *AtomicOpCore, store *atomicOpStore) *AtomicOpCtx {
	return &AtomicOpCtx{
		atomicOpCore:  core,
		atomicOpStore: store,
		closed:        false,
	}
}

func newAtomicOpStore() *atomicOpStore {
	return &atomicOpStore{store: make(map[string]any)}
}

func (a *AtomicOpCtx) Core() *AtomicOpCore {
	return a.atomicOpCore
}

func (s *atomicOpStore) SetValue(k fmt.Stringer, v any) {
	s.store[k.String()] = v
}

func (s *atomicOpStore) Value(k fmt.Stringer) any {
	return s.store[k.String()]
}

func (a *AtomicOpCtx) close() {
	a.closed = true
}
