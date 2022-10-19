package mapper

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const parallelProcessingLimit = 16

type ObjectReadListener interface {
	OnObjectRead(ctx context.Context, readCtx ReadContext, objectCtx ObjectContext) error
}

type ObjectReadListenerAllowIgnored interface {
	AllowIgnoredObject() bool
}

type ObjectReadListenerAllowWithError interface {
	AllowObjectWithError() bool
}

type AfterObjectsReadListener interface {
	AfterObjectsRead(ctx context.Context, readCtx AfterReadContext) error
}

type ObjectContext interface {
	Object() model.Object
	Directory() filesystem.Fs
	Path() model.AbsPath
	Parents() ObjectParents
	SetAnnotation(key string, value any)
	GetAnnotation(key string) any
	IsProcessed() <-chan struct{}
	Error() error
	SetError(error)
	IsIgnored() bool
	Ignore(reason string)
	UnIgnore()
	// markProcessed can be called only internally
	markProcessed()
}

type ReadContext interface {
	FileLoader() model.FilesLoader
	Naming() *naming.Generator
}

type AfterReadContext interface {
	ReadContext
	Objects() model.Object
}

type ObjectsStream interface {
	Add(parent ObjectContext, object model.Object, path model.AbsPath) ObjectContext
	Next() <-chan ObjectContext
}

type ObjectParents interface {
	ByKey(key model.Key) ObjectContext
}

type Mapper struct {
	tracer    trace.Tracer
	listeners []any
}

type dependencies interface {
	Tracer() trace.Tracer
}

func NewMapper(d dependencies, listeners ...any) *Mapper {
	v := &Mapper{tracer: d.Tracer()}
	for _, l := range listeners {
		switch l.(type) {
		case ObjectReadListener:
			// ok
		case AfterObjectsReadListener:
			// ok
		default:
			panic(fmt.Errorf("listener for local read mapper must be ObjectReadListener or AfterObjectsReadListener, found %T", l))
		}
	}
	return v
}

func (m *Mapper) ProcessObjects(ctx context.Context, readCtx ReadContext, objectsStream ObjectsStream) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// OnObjectRead
	err := m.invokeObjectReadListeners(ctx, readCtx, objectsStream)
	if err != nil {
		return err
	}

	// AfterObjectsRead
	if err := m.invokeAfterObjectsReadListeners(ctx, readCtx); err != nil {
		return err
	}

	// Errors
	errs := errors.NewMultiError()
	return errs.ErrorOrNil()
}

func (m *Mapper) invokeObjectReadListeners(ctx context.Context, readCtx ReadContext, objectsStream ObjectsStream) (err error) {
	ctx, span := m.tracer.Start(ctx, "state.backend.local.read.OnObjectReadListeners")
	defer telemetry.EndSpan(span, &err)

	// Limits the number of objects processed in parallel,
	// it prevents too many open files.
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(parallelProcessingLimit)

	// Invoke OnObjectRead listeners for each object.
	// Objects are processed in parallel, but listeners are called sequentially over the object.
	for {
		select {
		// Check context
		case <-ctx.Done():
			return ctx.Err()
		// Wait for next object or end of stream
		case objectCtx, found := <-objectsStream.Next():
			// Object processing can generate some new sub-object, for example a phase in an orchestrator.
			//   - Code is processing objectCtxOne.
			//   - Some listener finds a sub-object:
			//     - The listener calls ObjectsStream.Add(...) method.
			//     - It creates objectCtxTwo.
			//     - The same process is repeated with objectCtxTwo.
			//   - Worker function calls objectCtxOne.markProcessed() by defer,
			//     when all listeners have been invoked for the object.
			// So if all objects are processed, then no new ones can be found, and the loop can safely end.
			if !found {
				// All objects have been processed.
				return grp.Wait()
			}
			// Process next object in parallel
			m.goInvokeObjectReadListener(ctx, grp, readCtx, objectCtx)
		}
	}
}

func (m *Mapper) goInvokeObjectReadListener(ctx context.Context, grp *errgroup.Group, readCtx ReadContext, objectCtx ObjectContext) {
	// Objects are processed in parallel, but listeners are called sequentially for the object
	grp.Go(func() (err error) {
		ctx, span := m.tracer.Start(ctx, "state.backend.local.read.OnObjectReadListener")
		defer telemetry.EndSpan(span, &err)
		defer objectCtx.markProcessed()

		// Iterate over listeners
		for _, listener := range m.listeners {
			select {
			// Check context
			case <-ctx.Done():
				return ctx.Err()
			default:
				// ok, continue
			}

			if l, ok := listener.(ObjectReadListener); ok {
				if objectCtx.IsIgnored() {
					// Skip ignored object, if object is ignored and the listener does not support it
					l, ok := l.(ObjectReadListenerAllowIgnored)
					includeIgnored := ok && l.AllowIgnoredObject()
					if !includeIgnored {
						// Skip listener, object is ignored
						continue
					}
				}

				// Skip object with error, if object has error and the listener does not support it
				if objectCtx.Error() != nil {
					l, ok := l.(ObjectReadListenerAllowWithError)
					includeWithError := ok && l.AllowObjectWithError()
					if !includeWithError {
						// Skip listener, object has error
						continue
					}
				}

				// Invoke listener
				if err := l.OnObjectRead(ctx, readCtx, objectCtx); err != nil {
					objectCtx.SetError(err)
				}
			}
		}

		// No context error.
		//
		// If an error occurs during object processing by a listener,
		// it is stored by objectCtx.SetError() and it will be processed
		// at the end (or handled by another listener).
		return nil
	})
}

func (m *Mapper) invokeAfterObjectsReadListeners(ctx context.Context, readCtx ReadContext, objects ObjectsStream) (err error) {
	ctx, span := m.tracer.Start(ctx, "state.backend.local.read.AfterObjectsReadListeners")
	defer telemetry.EndSpan(span, &err)

	// AfterObjectsRead are called sequentially over the all objects.
	for _, listener := range m.listeners {
		if v, ok := listener.(AfterObjectsReadListener); ok {
			if err := v.AfterObjectsRead(ctx, objects); err != nil {
				errs.Append(err)
			}
		}
	}
}
