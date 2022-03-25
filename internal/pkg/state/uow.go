package state

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// UnitOfWork performed on the collection of objects.
// It is common abstraction for local and remote operations, it:
//   - Performs validation and then calls operation on UnitOfWorkBackend.
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
	Invoke(changes *model.Changes) error
	LoadAll(onLoad func(object model.Object) bool)
	Save(object model.Object, changedFields model.ChangedFields, objectExists bool, onSuccess func())
	Delete(key model.Key, onSuccess func())
}

// uow implements UnitOfWork interface.
type uow struct {
	invoked bool
	ctx     context.Context
	objects model.Objects
	filter  model.ObjectsFilter
	backend UnitOfWorkBackend
	changes *model.Changes
	errors  *utils.MultiError
}

func NewUnitOfWork(ctx context.Context, objects model.Objects, filter model.ObjectsFilter, backend UnitOfWorkBackend) UnitOfWork {
	return &uow{ctx: ctx, objects: objects, filter: filter, backend: backend, changes: model.NewChanges(), errors: utils.NewMultiError()}
}

// Invoke planned work in parallel.
func (u *uow) Invoke() error {
	// UoW can be invoked only once
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be reused`))
	}
	u.invoked = true

	// Invoke and merge backend level and common level errors
	errors := utils.NewMultiError()
	if err := u.backend.Invoke(u.changes); err != nil {
		errors.Append(err)
	}
	if err := u.errors.ErrorOrNil(); err != nil {
		errors.Append(err)
	}
	return errors.ErrorOrNil()
}

// LoadAll objects from the backend.
func (u *uow) LoadAll() {
	// Use backend
	u.backend.LoadAll(func(object model.Object) bool {
		// Is object ignored?
		if u.filter.IsObjectIgnored(object) {
			return false
		}

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

	// Use backend
	u.backend.Save(object, changedFields, exists, func() {
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

	})
}

// Delete object.
func (u *uow) Delete(key model.Key) {
	// Use backend
	u.backend.Delete(key, func() {
		// Remove object from the collection
		u.objects.Remove(key)

		// Add entry to changed list
		u.changes.AddDeleted(key)
	})
}
