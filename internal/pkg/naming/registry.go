package naming

import (
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	// maxFilenameLength is the maximum length for a filename component on most filesystems.
	// This is 255 bytes on Linux/Unix filesystems (ext4, XFS, etc.).
	maxFilenameLength = 255
	// suffixReservedLength reserves space for uniqueness suffix like "-999".
	suffixReservedLength = 4
)

type Registry struct {
	lock   *sync.Mutex
	byPath map[string]model.Key     // path -> object key
	byKey  map[string]model.AbsPath // object key -> path
}

func NewRegistry() *Registry {
	return &Registry{
		lock:   &sync.Mutex{},
		byPath: make(map[string]model.Key),
		byKey:  make(map[string]model.AbsPath),
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

	// Truncate the filename if it exceeds the maximum length, leaving room for suffix.
	// Use UTF-8 safe truncation to avoid splitting multibyte characters.
	if len(file) > maxFilenameLength-suffixReservedLength {
		file = truncateUTF8(file, maxFilenameLength-suffixReservedLength)
		// Update the path with the truncated filename
		p.SetRelativePath(filesystem.Join(dir, file))
	}

	// Add a suffix to the path if it is not unique
	suffix := 0
	for {
		foundKey, found := r.byPath[p.Path()]
		if !found || foundKey == key {
			break
		}

		suffix++
		suffixStr := fmt.Sprintf(`-%03d`, suffix)

		// Normalize the filename when adding a suffix.
		// Normalize first, then truncate to ensure final length is within limits.
		normalized := strhelper.NormalizeName(file + suffixStr)

		// Ensure the filename with suffix doesn't exceed the maximum length
		if len(normalized) > maxFilenameLength {
			// Truncate the file part first, then add suffix and normalize
			maxBaseLen := maxFilenameLength - len(suffixStr)
			truncatedFile := truncateUTF8(file, maxBaseLen)
			normalized = strhelper.NormalizeName(truncatedFile + suffixStr)

			// If still too long after normalization, truncate the final result
			if len(normalized) > maxFilenameLength {
				normalized = truncateUTF8(normalized, maxFilenameLength)
			}
		}

		p.SetRelativePath(filesystem.Join(dir, normalized))
	}
	return p
}

// truncateUTF8 truncates a string to at most maxBytes bytes,
// ensuring we don't split a multibyte UTF-8 character.
// Note: The suffix format is limited to -999 (3 digits). If more than 999 paths
// collide, subsequent paths will continue incrementing but may exceed the limit.
func truncateUTF8(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}

	// Find the last valid UTF-8 boundary at or before maxBytes
	for maxBytes > 0 && !utf8.RuneStart(s[maxBytes]) {
		maxBytes--
	}

	return s[:maxBytes]
}
