package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

// AtomicOpCore provides a common interface of the atomic operation, without result type specific methods.
// See the AtomicOp.Core method for details.
type AtomicOpCore struct {
	client     etcd.KV
	readPhase  []HighLevelFactory
	writePhase []HighLevelFactory
	locks          []Mutex
}

func (v *AtomicOpCore) AddFrom(ops ...AtomicOpInterface) *AtomicOpCore {
	for _, op := range ops {
		v.readPhase = append(v.readPhase, op.ReadPhaseOps()...)
		v.writePhase = append(v.writePhase, op.WritePhaseOps()...)
	}
	return v
}

func (v *AtomicOpCore) RequireLock(lock Mutex) *AtomicOpCore {
	v.locks = append(v.locks, lock)
	return v
}

func (v *AtomicOpCore) ReadOp(ops ...Op) *AtomicOpCore {
	for _, op := range ops {
		v.Read(func(ctx context.Context) Op {
			return op
		})
	}
	return v
}

func (v *AtomicOpCore) Read(factories ...func(ctx context.Context) Op) *AtomicOpCore {
	for _, fn := range factories {
		v.ReadOrErr(func(ctx context.Context) (Op, error) {
			return fn(ctx), nil
		})
	}
	return v
}

func (v *AtomicOpCore) OnRead(fns ...func(ctx context.Context)) *AtomicOpCore {
	for _, fn := range fns {
		v.ReadOrErr(func(ctx context.Context) (Op, error) {
			fn(ctx)
			return nil, nil
		})
	}
	return v
}

func (v *AtomicOpCore) OnReadOrErr(fns ...func(ctx context.Context) error) *AtomicOpCore {
	for _, fn := range fns {
		v.ReadOrErr(func(ctx context.Context) (Op, error) {
			return nil, fn(ctx)
		})
	}
	return v
}

func (v *AtomicOpCore) ReadOrErr(factories ...HighLevelFactory) *AtomicOpCore {
	v.readPhase = append(v.readPhase, factories...)
	return v
}

func (v *AtomicOpCore) Write(factories ...func(ctx context.Context) Op) *AtomicOpCore {
	for _, fn := range factories {
		v.WriteOrErr(func(ctx context.Context) (Op, error) {
			return fn(ctx), nil
		})
	}
	return v
}

func (v *AtomicOpCore) BeforeWrite(fns ...func(ctx context.Context)) *AtomicOpCore {
	for _, fn := range fns {
		v.WriteOrErr(func(ctx context.Context) (Op, error) {
			fn(ctx)
			return nil, nil
		})
	}
	return v
}

func (v *AtomicOpCore) BeforeWriteOrErr(fns ...func(ctx context.Context) error) *AtomicOpCore {
	for _, fn := range fns {
		v.WriteOrErr(func(ctx context.Context) (Op, error) {
			return nil, fn(ctx)
		})
	}
	return v
}

func (v *AtomicOpCore) WriteOp(ops ...Op) *AtomicOpCore {
	for _, op := range ops {
		v.Write(func(context.Context) Op {
			return op
		})
	}
	return v
}

func (v *AtomicOpCore) WriteOrErr(factories ...HighLevelFactory) *AtomicOpCore {
	v.writePhase = append(v.writePhase, factories...)
	return v
}
