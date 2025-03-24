package naming

import (
	"fmt"

	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type Registry struct {
	lock   *deadlock.Mutex
	byPath map[string]Key     // path -> object key
	byKey  map[string]AbsPath // object key -> path
}

func NewRegistry() *Registry {
	return &Registry{
		lock:   &deadlock.Mutex{},
		byPath: make(map[string]Key),
		byKey:  make(map[string]AbsPath),
	}
}

// Attach object's path to NamingTemplate, it guarantees the path will remain unique and will not be used again.
func (r Registry) Attach(key model.Key, path model.AbsPath) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Object path cannot be empty
	pathStr := path.Path()
	if len(pathStr) == 0 {
		return errors.Errorf(`naming error: invalid %s: path cannot be empty`, key.Desc())
	}

	// Check if the path is unique
	if foundKey, found := r.byPath[pathStr]; found && foundKey != key {
		return errors.Errorf(
			`naming error: path "%s" is attached to %s, but new %s has same path`,
			pathStr, foundKey.Desc(), key.Desc())
	}

	// Remove the previous value attached to the key
	if foundPath, found := r.byKey[key.String()]; found {
		delete(r.byPath, foundPath.Path())
	}

	r.byPath[pathStr] = key
	r.byKey[key.String()] = path
	return nil
}

// Detach object's path from NamingTemplate, so it can be used by other object.
func (r Registry) Detach(key model.Key) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if foundPath, found := r.byKey[key.String()]; found {
		delete(r.byPath, foundPath.Path())
		delete(r.byKey, key.String())
	}
}

func (r Registry) PathByKey(key model.Key) (model.AbsPath, bool) {
	path, found := r.byKey[key.String()]
	return path, found
}

func (r Registry) KeyByPath(path string) (model.Key, bool) {
	key, found := r.byPath[path]
	return key, found
}

func (r Registry) ensureUniquePath(key model.Key, p model.AbsPath) model.AbsPath {
	p = r.makeUniquePath(key, p)
	if err := r.Attach(key, p); err != nil {
		panic(err)
	}
	return p
}

func (r Registry) makeUniquePath(key model.Key, p model.AbsPath) model.AbsPath {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Object path cannot be empty
	if len(p.GetRelativePath()) == 0 {
		p.SetRelativePath(strhelper.NormalizeName(key.Kind().Name))
	}

	dir, file := filesystem.Split(p.GetRelativePath())

	// Add a suffix to the path if it is not unique
	suffix := 0
	for {
		foundKey, found := r.byPath[p.Path()]
		if !found || foundKey == key {
			break
		}

		suffix++
		p.SetRelativePath(filesystem.Join(dir, strhelper.NormalizeName(file+"-"+fmt.Sprintf(`%03d`, suffix))))
	}
	return p
}
