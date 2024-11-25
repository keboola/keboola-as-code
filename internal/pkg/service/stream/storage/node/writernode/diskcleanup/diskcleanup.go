// Package diskcleanup provides removing of all expired slices from the local storage disk.
// Expired slice has a local directory, but there is no DB record for it.
package diskcleanup

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/slice"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Node struct {
	config     Config
	clock      clock.Clock
	logger     log.Logger
	telemetry  telemetry.Telemetry
	repository *storageRepo.Repository
	volumes    *diskwriter.Volumes
}

type volumeState struct {
	VolumeID   volume.ID
	VolumePath string
	// DiskDirs relative to the volume path.
	DiskDirs []string
	// DBDirs relative to the volume path.
	DBDirs map[string]bool
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	StorageRepository() *storageRepo.Repository
	Volumes() *diskwriter.Volumes
}

func Start(d dependencies, cfg Config) error {
	n := &Node{
		config:     cfg,
		clock:      d.Clock(),
		logger:     d.Logger().WithComponent("storage.disk.cleanup"),
		telemetry:  d.Telemetry(),
		repository: d.StorageRepository(),
		volumes:    d.Volumes(),
	}

	ctx := context.Background()
	if !n.config.Enabled {
		n.logger.Info(ctx, "local storage disk cleanup is disabled")
		return nil
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		n.logger.Info(ctx, "received shutdown request")
		cancel()
		wg.Wait()
		n.logger.Info(ctx, "shutdown done")
	})

	// Start timer
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := d.Clock().Ticker(n.config.Interval)
		defer ticker.Stop()

		for {
			if err := n.cleanDisk(ctx); err != nil && !errors.Is(err, context.Canceled) {
				n.logger.Errorf(ctx, `local storage disk cleanup failed: %s`, err)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
	}()

	return nil
}

// cleanDisk iterates directories in the local storage, and delete those without a File record in DB.
func (n *Node) cleanDisk(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
	defer cancel()

	ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.cleanup.disk.cleanDisk")
	defer span.End(&err)

	// Measure count of deleted files
	counter := atomic.NewInt64(0)
	defer func() {
		count := counter.Load()
		span.SetAttributes(attribute.Int64("removedDirectoriesCount", count))
		n.logger.With(attribute.Int64("removedDirectoriesCount", count)).Info(ctx, `removed "<removedDirectoriesCount>" directories`)
	}()

	// Check/remove directories in parallel, but with limit
	n.logger.Info(ctx, `removing expired files without DB record from disk`)
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(n.config.Concurrency)

	// Get volumes attached to the node
	volumes := make(map[volume.ID]*volumeState)
	for _, vol := range n.volumes.Collection().All() {
		// Get existing dir dirs BEFORE the state from the database!
		// Otherwise, we could delete some new directory that exists on the disk,
		// but does not exist in the old snapshot of the database.
		diskDirs, err := n.volumeDiskDirs(vol)
		if err != nil {
			return err
		}
		volumes[vol.ID()] = &volumeState{
			VolumeID:   vol.ID(),
			VolumePath: vol.Path(),
			DiskDirs:   diskDirs,
			DBDirs:     make(map[string]bool),
		}
	}

	// Add list of existing slices from DB
	err = n.repository.Slice().ListAll().ForEach(func(s model.Slice, _ *iterator.Header) error {
		if vol := volumes[s.VolumeID]; vol != nil {
			vol.DBDirs[s.LocalStorage.DirName(vol.VolumePath)] = true
		}
		return nil
	}).Do(ctx).Err()
	if err != nil {
		return err
	}

	// We have to later remove empty parent dirs
	var parentDirsLock sync.Mutex
	parentDirsMap := make(map[string]bool)

	// Remove dirs present only on the disk, not in DB
	grp.Go(func() error {
		for _, vol := range volumes {
			for _, path := range vol.DiskDirs {
				if !vol.DBDirs[path] {
					// Remove dir
					grp.Go(func() error {
						ctx = ctxattr.ContextWith(
							ctx,
							attribute.String("volume.ID", vol.VolumeID.String()),
							attribute.String("path", path),
						)

						if err := os.RemoveAll(path); err != nil {
							n.logger.Errorf(ctx, "cannot remove directory %q: %s", path, err.Error())
							return err
						}

						n.logger.Debugf(ctx, "removed directory %q", path)
						counter.Add(1)

						// Store parent dirs to check if they are not empty
						parentDir := path
						for {
							parentDir = filepath.Dir(parentDir)

							// Don't go over the volume path
							if !strings.HasPrefix(parentDir, vol.VolumePath+string(filepath.Separator)) {
								break
							}

							// Store the path and try parent dir
							parentDirsLock.Lock()
							parentDirsMap[parentDir] = true
							parentDirsLock.Unlock()
						}

						return nil
					})
				}
			}
		}
		return nil
	})

	if err := grp.Wait(); err != nil {
		return err
	}

	if err := n.removeEmptyDirs(ctx, parentDirsMap); err != nil {
		return err
	}

	return nil
}

func (n *Node) volumeDiskDirs(volume *diskwriter.Volume) (out []string, err error) {
	root := volume.Path()
	err = filepath.WalkDir(root, func(absPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Get relative path of the dir
			relPath, err := filepath.Rel(root, absPath)
			if err != nil {
				return err
			}

			// Collect the path if the depth match
			if strings.Count(relPath, string(os.PathSeparator)) == slice.DirPathSegments-1 {
				out = append(out, absPath)

				// Don't go deeper
				return fs.SkipDir
			}
			return nil
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (n *Node) removeEmptyDirs(ctx context.Context, dirsMap map[string]bool) error {
	dirs := make([]string, 0, len(dirsMap))
	for dir := range dirsMap {
		dirs = append(dirs, dir)
	}

	// Sort most nested path first
	sort.SliceStable(dirs, func(i, j int) bool {
		return len(dirs[i]) > len(dirs[j])
	})

	for _, dir := range dirs {
		if err := n.removeDirIfEmpty(ctx, dir); err != nil {
			return errors.PrefixErrorf(err, "cannot remove empty dir %q", dir)
		}
	}

	return nil
}

func (n *Node) removeDirIfEmpty(ctx context.Context, path string) error {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := fd.Close(); err != nil {
			n.logger.Errorf(ctx, "cannot close dir %q", path)
		}
	}()

	// Check if the dir is empty
	if _, err = fd.Readdirnames(1); errors.Is(err, io.EOF) {
		// Remove dir
		if err := os.Remove(path); err != nil {
			return err
		}
	}

	return nil
}
