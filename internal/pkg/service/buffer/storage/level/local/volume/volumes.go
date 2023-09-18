// Package volume contains common code for reader.Volumes and writer.Volumes implementations.
// The DetectVolumes function detects and opens all volumes in the volumesPath.
//
// Volume relative path has the following format: "{type}/{label}".
//
// The type is later used when assigning volumes.
// Different use-cases may prefer a different type of volume.
//
// The label has no special meaning, volumes are identified by the unique storage.VolumeID,
// which is read from the IDFile on the volume, if the file does not exist,
// it is generated and saved by the writer.Volume.
package volume

import (
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const IDFile = "volume-id"

// Volume instance common interface.
type Volume interface {
	Path() string
	Type() string
	Label() string
	ID() storage.VolumeID
	Close() error
}

// Volumes manages volumes in the path, each instance has V type.
type Volumes[V Volume] struct {
	logger  log.Logger
	path    string
	factory Opener[V]
	byID    map[storage.VolumeID]V
	byType  map[string][]V
}

// Opener opens volume instance of the V type.
type Opener[V Volume] func(info Info) (V, error)

// DetectVolumes function detects and opens all volumes in the volumesPath.
// It is an abstract implementation, the opening of volumes is delegated to the Opener.
func DetectVolumes[V Volume](logger log.Logger, volumesPath string, opener Opener[V]) (*Volumes[V], error) {
	m := &Volumes[V]{
		logger:  logger,
		path:    volumesPath,
		factory: opener,
		byID:    make(map[storage.VolumeID]V),
		byType:  make(map[string][]V),
	}

	if err := m.detect(); err != nil {
		return nil, err
	}

	return m, nil
}

// Volume returns the volume instance by the ID or an error if it is not found.
func (v *Volumes[V]) Volume(id storage.VolumeID) (V, error) {
	if v, ok := v.byID[id]; ok {
		return v, nil
	} else {
		var empty V
		return empty, errors.Errorf(`volume with ID "%s" not found`, id)
	}
}

// VolumeByType returns volumes which match the type.
func (v *Volumes[V]) VolumeByType(typ string) (out []V) {
	out = make([]V, len(v.byType[typ]))
	copy(out, v.byType[typ])
	sortVolumes(out)
	return out
}

func (v *Volumes[V]) All() (out []V) {
	i := 0
	out = make([]V, len(v.byID))
	for _, v := range v.byID {
		out[i] = v
		i++
	}
	sortVolumes(out)
	return out
}

// Close all volumes.
func (v *Volumes[V]) Close() error {
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}
	for _, v := range v.byID {
		v := v
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := v.Close(); err != nil {
				errs.Append(err)
			}
		}()
	}
	wg.Wait()
	return errs.ErrorOrNil()
}

// detect volumes in the volumesPath.
// Relative path of each Volume has format: {type}/{label}.
func (v *Volumes[V]) detect() error {
	v.logger.Infof(`searching for volumes in "%s"`, v.path)

	lock := &sync.Mutex{}
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}
	idCount := make(map[storage.VolumeID]int)
	walkErr := filepath.WalkDir(v.path, func(path string, d fs.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Skip files
		if !d.IsDir() {
			return nil
		}

		// Volume relative path has 2 parts: {type}/{label}
		relPath := filepath.ToSlash(strings.TrimPrefix(path, v.path+string(filepath.Separator)))
		if parts := strings.Split(relPath, "/"); len(parts) == 2 {
			// Open volume in parallel
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Create reference
				typ, label := parts[0], parts[1]
				typ = strings.ToLower(typ)
				v.logger.Infof(`found volume, type="%s", path="%s"`, typ, label)

				// Open the volume
				info := NewInfo(path, typ, label)
				vol, err := v.factory(info)
				if err == nil {
					v.logger.Infof(`opened volume, id="%s", type="%s", path="%s"`, vol.ID(), vol.Type(), vol.Label())
				} else {
					v.logger.Errorf(`cannot open volume, type="%s", path="%s": %s`, err)
					errs.Append(err)
					return
				}

				// Register the volume
				lock.Lock()
				defer lock.Unlock()

				// Register volume
				id := vol.ID()
				idCount[id]++
				v.byID[id] = vol
				v.byType[vol.Type()] = append(v.byType[vol.Type()], vol)
			}()

			// Don't go deeper
			return filepath.SkipDir
		}

		// Go deeper
		return nil
	})

	// Wait for all volumes opening
	wg.Wait()

	// Each volume ID must be unique
	for id, count := range idCount {
		if count > 1 {
			return errors.Errorf(`found %d volumes with the ID "%s"`, count, id)
		}
	}

	// Check walk error
	if walkErr != nil {
		errs.Append(walkErr)
	}

	// Check errors
	if err := errs.ErrorOrNil(); err != nil {
		return err
	}

	// At least one volume must be found
	if len(v.byID) == 0 {
		return errors.New("no volume found")
	}

	v.logger.Infof(`found "%d" volumes`, len(v.byID))
	return nil
}

func sortVolumes[V Volume](v []V) {
	sort.SliceStable(v, func(i, j int) bool {
		if v := strings.Compare(v[i].Type(), v[j].Type()); v != 0 {
			return v < 0
		}
		return strings.Compare(v[i].Label(), v[j].Label()) < 0
	})
}
