package local

import (
	"fmt"
	"github.com/otiai10/copy"
	"go.uber.org/zap"
	"keboola-as-code/src/utils"
	"os"
)

type RenamePlan struct {
	OldPath     string
	NewPath     string
	Description string
}

// Rename according to the defined plan
func Rename(plan []*RenamePlan, logger *zap.SugaredLogger) (warns error, errs error) {
	errors := utils.NewMultiError()
	warnings := utils.NewMultiError()
	pathsToRemove := make([]string, 0)

	// Execute plan
	if len(plan) > 0 {
		logger.Debugf(`Starting renaming of the %d paths.`, len(plan))
		logger.Info("Renamed objects:")
		for _, item := range plan {
			// Deep copy
			err := copy.Copy(item.OldPath, item.NewPath, copy.Options{
				OnDirExists:   func(src, dest string) copy.DirExistsAction { return copy.Replace },
				Sync:          true,
				PreserveTimes: true,
			})

			if err != nil {
				errors.AppendWithPrefix(fmt.Sprintf(`cannot copy \"%s\"`, item.Description), err)
			} else {
				// Log info
				logger.Info("\t- ", item.Description)

				// Remove old path
				pathsToRemove = append(pathsToRemove, item.OldPath)
			}
		}
	} else {
		logger.Debug(`No path to rename.`)
	}

	// Remove old paths
	if errors.Len() == 0 {
		for _, oldPath := range pathsToRemove {
			if err := os.RemoveAll(oldPath); err != nil {
				warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, oldPath), err)
			}
		}
	}

	return warnings.ErrorOrNil(), errors.ErrorOrNil()
}
