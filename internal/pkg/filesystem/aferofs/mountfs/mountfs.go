package mountfs

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/abstract"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/basepathfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MountFs allows you to mount a directory in a filesystem.
type MountFs struct {
	root     abstract.Backend
	basePath string
	mounts   []MountPoint
	utils    *afero.Afero
}

type MountPoint struct {
	Path string
	Fs   abstract.Backend
}

// file wrapper overwrites Readdir and Readdirnames to include mount points.
type file struct {
	afero.File
	fs   *MountFs
	path string
}

func New(root abstract.Backend, basePath string, mounts ...MountPoint) *MountFs {
	fs := &MountFs{}
	fs.root = root
	fs.basePath = basePath
	fs.utils = &afero.Afero{Fs: fs}
	fs.mounts = mounts
	fs.sortMounts()
	return fs
}

func NewMountPoint(path string, fs filesystem.Fs) MountPoint {
	return MountPoint{Path: path, Fs: fs.(abstract.BackendProvider).Backend()}
}

func (v *MountFs) Name() string {
	return v.root.Name()
}

func (v *MountFs) BasePath() string {
	return v.basePath
}

func (v *MountFs) SubDirFs(path string) (any, error) {
	targetFs, _, targetPath := v.fsFor(path)

	// Get backend
	var subFsBackend abstract.Backend
	if targetPath == "" {
		// Sub filesystem is root of a mount point
		subFsBackend = targetFs
	} else if backend, err := basepathfs.New(targetFs, targetPath); err == nil {
		// Sub filesystem is a sub dir (from root filesystem or a mount point)
		subFsBackend = backend
	} else {
		return nil, err
	}

	// Copy mount points
	var mounts []MountPoint
	for _, m := range v.mounts {
		if mountPath, err := filesystem.Rel(v.ToSlash(path), m.Path); err == nil && mountPath != "" {
			mounts = append(mounts, MountPoint{mountPath, m.Fs})
		}
	}

	// Create filesystem
	basePath := filepath.Join(v.BasePath(), path) // nolint: forbidigo
	return New(subFsBackend, basePath, mounts...), nil
}

// FromSlash returns OS representation of the path.
func (v *MountFs) FromSlash(path string) string {
	return v.root.FromSlash(path)
}

// ToSlash returns internal representation of the path.
func (v *MountFs) ToSlash(path string) string {
	return v.root.ToSlash(path)
}

func (v *MountFs) Walk(root string, walkFn filesystem.WalkFunc) error {
	return v.utils.Walk(root, walkFn)
}

func (v *MountFs) ReadDir(path string) ([]filesystem.FileInfo, error) {
	return v.utils.ReadDir(path)
}

func (v *MountFs) Chtimes(name string, a, m time.Time) error {
	targetFs, _, relPath := v.fsFor(name)
	return targetFs.Chtimes(relPath, a, m)
}

func (v *MountFs) Chmod(name string, mode os.FileMode) error {
	targetFs, _, relPath := v.fsFor(name)
	return targetFs.Chmod(relPath, mode)
}

func (v *MountFs) Chown(name string, uid, gid int) error {
	targetFs, _, relPath := v.fsFor(name)
	return targetFs.Chown(relPath, uid, gid)
}

func (v *MountFs) Stat(name string) (os.FileInfo, error) {
	targetFs, _, relPath := v.fsFor(name)
	return targetFs.Stat(relPath)
}

func (v *MountFs) Rename(oldName, newName string) error {
	targetFs, mountDir, _ := v.fsFor(oldName)

	oldName = v.ToSlash(oldName)
	oldRel, err := filesystem.Rel(mountDir, oldName)
	if err != nil {
		return err
	}

	newName = v.ToSlash(newName)
	newRel, err := filesystem.Rel(mountDir, newName)
	if err != nil {
		return errors.Errorf(`path "%s" cannot be moved outside mount dir "%s" to "%s"`, oldName, mountDir, newName)
	}

	return targetFs.Rename(v.FromSlash(oldRel), v.FromSlash(newRel))
}

func (v *MountFs) RemoveAll(p string) error {
	targetFs, _, relPath := v.fsFor(p)
	return targetFs.RemoveAll(relPath)
}

func (v *MountFs) Remove(name string) error {
	targetFs, _, relPath := v.fsFor(name)
	return targetFs.Remove(relPath)
}

func (v *MountFs) OpenFile(path string, flag int, perm os.FileMode) (afero.File, error) {
	targetFs, mountDir, relPath := v.fsFor(path)
	f, err := targetFs.OpenFile(relPath, flag, perm)
	if err != nil {
		return nil, err
	}
	return file{File: f, fs: v, path: filesystem.Join(mountDir, v.ToSlash(f.Name()))}, nil
}

func (v *MountFs) Open(path string) (afero.File, error) {
	targetFs, mountDir, relPath := v.fsFor(path)
	f, err := targetFs.Open(relPath)
	if err != nil {
		return nil, err
	}
	return file{File: f, fs: v, path: filesystem.Join(mountDir, v.ToSlash(f.Name()))}, nil
}

func (v *MountFs) Mkdir(path string, p os.FileMode) error {
	targetFs, _, relPath := v.fsFor(path)
	return targetFs.Mkdir(relPath, p)
}

func (v *MountFs) MkdirAll(path string, p os.FileMode) error {
	targetFs, _, relPath := v.fsFor(path)
	return targetFs.MkdirAll(relPath, p)
}

func (v *MountFs) Create(name string) (afero.File, error) {
	targetFs, _, relPath := v.fsFor(name)
	return targetFs.Create(relPath)
}

// sortMounts mounts, the longest paths will be first.
// This ensures that the child path is always tested before the parent path, if both are mount points.
func (v *MountFs) sortMounts() {
	sort.SliceStable(v.mounts, func(i, j int) bool {
		return len(v.mounts[i].Path) > len(v.mounts[j].Path)
	})
}

// fsFor finds the nearest mount point or returns the root filesystem.
func (v *MountFs) fsFor(path string) (targetFs abstract.Backend, mountDir, relPath string) {
	for _, m := range v.mounts {
		if rel, err := filesystem.Rel(m.Path, v.ToSlash(path)); err == nil {
			return m.Fs, m.Path, rel
		}
	}
	return v.root, "", path
}

// Readdir adds mount points to the result if there are any in the directory.
func (f file) Readdir(count int) ([]os.FileInfo, error) {
	// Read items from FS
	files, err := f.File.Readdir(count)
	if err != nil {
		return nil, err
	}

	// Convert slice to map
	paths := make(map[string]bool)
	for _, fileInfo := range files {
		paths[fileInfo.Name()] = true
	}

	// Add mount points present in the directory
	for _, m := range f.fs.mounts {
		if count > 0 && len(files) >= count {
			// Max count reached
			break
		}
		if relPath, err := filesystem.Rel(f.path, m.Path); err == nil && len(relPath) > 0 {
			subDir := strings.SplitN(relPath, string(filesystem.PathSeparator), 2)[0]
			if !paths[subDir] {
				// Add item if it not already present
				paths[subDir] = true
				if stat, err := f.fs.Stat(f.fs.FromSlash(filesystem.Join(f.path, subDir))); err != nil {
					files = append(files, stat)
				}
			}
		}
	}
	return files, nil
}

// Readdirnames adds mount points to the result if there are any in the directory.
func (f file) Readdirnames(count int) ([]string, error) {
	// Read items from FS
	files, err := f.File.Readdirnames(count)
	if err != nil {
		return nil, err
	}

	// Convert slice to map
	paths := make(map[string]bool)
	for _, filePath := range files {
		paths[filePath] = true
	}

	// Add mount points present in the directory
	for _, m := range f.fs.mounts {
		if count > 0 && len(files) >= count {
			// Max count reached
			break
		}
		if relPath, err := filesystem.Rel(f.path, m.Path); err == nil && len(relPath) > 0 {
			subDir := strings.SplitN(relPath, string(filesystem.PathSeparator), 2)[0]
			if !paths[subDir] {
				// Add item if it not already present
				paths[subDir] = true
				files = append(files, f.fs.FromSlash(subDir))
			}
		}
	}
	return files, nil
}
