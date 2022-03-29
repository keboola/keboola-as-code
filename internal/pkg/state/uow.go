package state

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// UnitOfWork performed on the collection of objects.
// It is common abstraction for local and remote operations, it:
//   - Performs validation and then calls operation on the UnitOfWorkBackend.
//   - Modifies the collection of objects if the operation is successful.
//   - Generates set of changes.
type UnitOfWork interface {
	Invoke() error
	LoadAll()
	Save(object model.Object, changedFields model.ChangedFields)
	Delete(key model.Key)
}

// UnitOfWorkBackend for backend-specific code, the callback onSuccess is called after the successful operation.
type UnitOfWorkBackend interface {
	Invoke() (FinalizationFn, error)
	LoadAll(loadCtx LoadContext)
	Save(saveCtx SaveContext)
	Delete(deleteCtx DeleteContext)
}

// FinalizationFn performs finalization when the UnitOfWork is finished.
type FinalizationFn func(changes *model.Changes) error

// LoadContext is used for data exchange between the UnitOfWork and the UnitOfWorkBackend on load.
type LoadContext struct {
	OnLoad func(object model.Object) (accepted bool)
}

// SaveContext is used for data exchange between the UnitOfWork and the UnitOfWorkBackend on save.
type SaveContext struct {
	Object        model.Object
	ObjectExists  bool
	ChangedFields model.ChangedFields
	OnSuccess     func()
}

// DeleteContext is used for data exchange between the UnitOfWork and the UnitOfWorkBackend on delete.
type DeleteContext struct {
	Key       model.Key
	OnSuccess func()
}

// uow implements UnitOfWork interface.
type uow struct {
	invoked bool
	ctx     context.Context
	objects model.Objects
	backend UnitOfWorkBackend
	changes *model.Changes
	errors  *utils.MultiError
}

func NewUnitOfWork(ctx context.Context, objects model.Objects, backend UnitOfWorkBackend) UnitOfWork {
	return &uow{ctx: ctx, objects: objects, backend: backend, changes: model.NewChanges(), errors: utils.NewMultiError()}
}

// Invoke planned work in parallel.
func (u *uow) Invoke() error {
	// UoW can be invoked only once
	if u.invoked {
		panic(fmt.Errorf(`UnitOfWork: invoke can only be called once`))
	}
	u.invoked = true

	// Invoke
	errors := utils.NewMultiError()
	finalizationFn, err := u.backend.Invoke()
	if err != nil {
		errors.Append(err)
	}

	// Finalize
	if err := finalizationFn(u.changes); err != nil {
		errors.Append(err)
	}

	// Add common errors
	if err := u.errors.ErrorOrNil(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}

// LoadAll objects from the backend.
func (u *uow) LoadAll() {
	// Use backend
	u.backend.LoadAll(LoadContext{
		OnLoad: func(object model.Object) (accepted bool) {
			// Validate
			if err := validator.Validate(u.ctx, object); err != nil {
				u.errors.AppendWithPrefix(fmt.Sprintf(`%s is invalid`, object.String()), err)
				return false
			}

			// Add object to the collection
			if err := u.objects.AddOrReplace(object); err != nil {
				u.errors.Append(err)
				return false
			}

			// Add entry to changed list
			u.changes.AddLoaded(object)
			return true
		},
	})
}

// Save object. Object will be created or updated.
func (u *uow) Save(object model.Object, changedFields model.ChangedFields) {
	// Check if object exists
	_, exists := u.objects.Get(object.Key())
	if !exists {
		changedFields = nil
	}

	// Validate
	if err := validator.Validate(u.ctx, object); err != nil {
		u.errors.AppendWithPrefix(fmt.Sprintf(`%s is invalid`, object.String()), err)
		return
	}

	// Clone object and create recipe
	// During the mapping is the internal object modified, so it is needed to clone it first.
	// The internal representation will thus remain unaffected.
	backendObject := deepcopy.Copy(object).(model.Object)

	// Use backend
	u.backend.Save(SaveContext{
		Object:        backendObject,
		ObjectExists:  exists,
		ChangedFields: changedFields,
		OnSuccess: func() {
			// Add object to the collection
			if err := u.objects.AddOrReplace(object); err != nil {
				u.errors.Append(err)
				return
			}

			// Add entry to changed list
			if exists {
				u.changes.AddUpdated(object)
			} else {
				u.changes.AddCreated(object)
			}
		},
	})
}

// Delete object.
func (u *uow) Delete(key model.Key) {
	// Use backend
	u.backend.Delete(DeleteContext{
		Key: key,
		OnSuccess: func() {
			// Remove object from the collection
			u.objects.Remove(key)

			// Add entry to changed list
			u.changes.AddDeleted(key)
		},
	})
}
