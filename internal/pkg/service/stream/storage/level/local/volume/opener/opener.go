// Package opener provides volumes detection in a configured volume path.
//
// Volume relative path has the following format: "{type}/{label}".
//
// The type is later used when assigning volumes.
// Different use-cases may prefer a different type of volume.
//
// The label has no special meaning, volumes are identified by the unique storage.ID,
// which is read from the local.VolumeIDFile on the volume, if the file does not exist,
// it is generated and saved by the writer.Volume.
package opener

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Opener opens volume reader or writer instance of the V type on a local node.
type Opener[V volume.Volume] func(spec volume.Spec) (V, error)

// OpenVolumes function detects and opens all volumes in the volumesPath.
// It is an abstract implementation, the opening of volumes is delegated to the Opener.
func OpenVolumes[V volume.Volume](ctx context.Context, logger log.Logger, nodeID, volumesPath string, opener Opener[V]) (*volume.Collection[V], error) {
	logger.Infof(ctx, `searching for volumes in "%s"`, volumesPath)

	lock := &sync.Mutex{}
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}

	var volumes []V
	walkErr := filepath.WalkDir(volumesPath, func(path string, d fs.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Skip files
		if !d.IsDir() {
			return nil
		}

		// Volume relative path has 2 parts: {type}/{label}
		relPath := filepath.ToSlash(strings.TrimPrefix(path, volumesPath+string(filepath.Separator)))
		if parts := strings.Split(relPath, "/"); len(parts) == 2 {
			// Open volume in parallel
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Create reference
				typ, label := parts[0], parts[1]
				typ = strings.ToLower(typ)
				logger.Infof(ctx, `found volume, type="%s", path="%s"`, typ, label)

				// Check volume directory
				if err = checkVolumeDir(path); err != nil {
					logger.Errorf(ctx, `cannot open volume, type="%s", path="%s": %s`, typ, path, err)
					errs.Append(err)
					return
				}

				// Open the volume
				info := volume.Spec{NodeID: nodeID, Path: path, Type: typ, Label: label}
				vol, err := opener(info)
				if err != nil {
					logger.Errorf(ctx, `cannot open volume, type="%s", path="%s": %s`, typ, path, err)
					errs.Append(err)
					return
				}

				// Register the volume
				lock.Lock()
				defer lock.Unlock()

				// Add volume
				volumes = append(volumes, vol)
			}()

			// Don't go deeper
			return filepath.SkipDir
		}

		// Go deeper
		return nil
	})

	// Wait for all volumes opening
	wg.Wait()

	// Check walk error
	if walkErr != nil {
		errs.Append(errors.PrefixErrorf(walkErr, `cannot walk volumes path "%s"`, volumesPath))
	}

	// Check errors
	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	// Create collection
	collection, err := volume.NewCollection[V](volumes)
	if err != nil {
		return nil, err
	}

	logger.Infof(ctx, `found "%d" volumes`, collection.Count())
	return collection, nil
}
