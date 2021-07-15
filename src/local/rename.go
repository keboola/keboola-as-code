package local

import (
	"fmt"
	"github.com/otiai10/copy"
	"go.uber.org/zap"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
)

type RenamePlan struct {
	OldPath     string
	NewPath     string
	Description string
}

func (p *RenamePlan) validate() error {
	if !filepath.IsAbs(p.OldPath) {
		return fmt.Errorf("old path must be absolute")
	}
	if !filepath.IsAbs(p.NewPath) {
		return fmt.Errorf("new path must be absolute")
	}
	return nil
}

// Rename according to the defined plan
func Rename(plan []*RenamePlan, logger *zap.SugaredLogger) (warns error, errs error) {
	errors := utils.NewMultiError()
	warnings := utils.NewMultiError()
	newPaths := make([]string, 0)
	pathsToRemove := make([]string, 0)

	// Execute plan
	if len(plan) > 0 {
		logger.Debugf(`Starting renaming of the %d paths.`, len(plan))
		logger.Info("Renamed objects:")
		for _, item := range plan {
			// Validate
			if err := item.validate(); err != nil {
				panic(err)
			}

			// Deep copy
			err := copy.Copy(item.OldPath, item.NewPath, copy.Options{
				OnDirExists:   func(src, dest string) copy.DirExistsAction { return copy.Replace },
				Sync:          true,
				PreserveTimes: true,
			})

			if err != nil {
				errors.AppendWithPrefix(fmt.Sprintf(`cannot copy "%s"`, item.Description), err)
			} else {
				// Log info
				logger.Info("\t- ", item.Description)

				// Remove old path
				newPaths = append(newPaths, item.NewPath)
				pathsToRemove = append(pathsToRemove, item.OldPath)
			}
		}
	} else {
		logger.Debug(`No path to rename.`)
	}

	if errors.Len() == 0 {
		// No error -> remove old paths
		for _, oldPath := range pathsToRemove {
			if err := os.RemoveAll(oldPath); err != nil {
				warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, oldPath), err)
			}
		}
	} else {
		// An error occurred -> keep old state -> remove new paths
		for _, newPath := range newPaths {
			if err := os.RemoveAll(newPath); err != nil {
				warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, newPath), err)
			}
		}
		logger.Info(`Error occurred, the rename operation was reverted.`)
	}

	return warnings.ErrorOrNil(), errors.ErrorOrNil()
}
