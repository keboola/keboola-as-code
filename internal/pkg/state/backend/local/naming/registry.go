package naming

import (
	"fmt"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type Registry struct {
	lock   *sync.Mutex
	byPath map[string]Key  // path -> object key
	byKey  map[Key]AbsPath // object key -> path
}

func NewRegistry() *Registry {
	return &Registry{
		lock:   &sync.Mutex{},
		byPath: make(map[string]Key),
		byKey:  make(map[Key]AbsPath),
	}
}

func (r Registry) All() map[Key]AbsPath {
	out := make(map[Key]AbsPath)
	for key, path := range r.byKey {
		out[key] = path
	}
	return out
}

func (r Registry) AllStrings() map[string]string {
	out := make(map[string]string)
	for key, path := range r.byKey {
		out[key.String()] = path.String()
	}
	return out
}

func (r Registry) Clear() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.byPath = make(map[string]Key)
	r.byKey = make(map[Key]AbsPath)
}

// Attach object's path to NamingTemplate, it guarantees the path will remain unique and will not be used again.
func (r Registry) Attach(key Key, path AbsPath) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Object path cannot be empty
	pathStr := path.String()
	if len(pathStr) == 0 {
		return fmt.Errorf(`naming error: invalid %s: path cannot be empty`, key.String())
	}

	// Check if the path is unique
	if foundKey, found := r.byPath[pathStr]; found && foundKey != key {
		return fmt.Errorf(
			`naming error: path "%s" is attached to %s, but new %s has same path`,
			pathStr, foundKey.String(), key.String(),
		)
	}

	// Remove the previous value attached to the key
	if foundPath, found := r.byKey[key]; found {
		delete(r.byPath, foundPath.String())
	}

	r.byPath[pathStr] = key
	r.byKey[key] = path
	return nil
}

func (r Registry) MustAttach(key Key, path AbsPath) {
	if err := r.Attach(key, path); err != nil {
		panic(err)
	}
}

// Detach object's path from NamingTemplate, so it can be used by other object.
func (r Registry) Detach(key Key) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if foundPath, found := r.byKey[key]; found {
		delete(r.byPath, foundPath.String())
		delete(r.byKey, key)
	}
}

func (r Registry) PathByKey(key Key) (AbsPath, bool) {
	path, found := r.byKey[key]
	return path, found
}

func (r Registry) KeyByPath(path string) (Key, bool) {
	key, found := r.byPath[path]
	return key, found
}

func (r Registry) ensureUniquePath(key Key, p AbsPath) AbsPath {
	p = r.makeUniquePath(key, p)
	if err := r.Attach(key, p); err != nil {
		panic(err)
	}
	return p
}

func (r Registry) makeUniquePath(key Key, p AbsPath) AbsPath {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Object path cannot be empty
	if len(p.RelativePath()) == 0 {
		p = p.WithRelativePath(strhelper.NormalizeName(key.Kind().Name))
	}

	dir, file := filesystem.Split(p.RelativePath())

	// Add a suffix to the path if it is not unique
	suffix := 0
	for {
		foundKey, found := r.byPath[p.String()]
		if !found || foundKey == key {
			break
		}

		suffix++
		p = p.WithRelativePath(filesystem.Join(dir, strhelper.NormalizeName(file+"-"+fmt.Sprintf(`%03d`, suffix))))
	}
	return p
}
