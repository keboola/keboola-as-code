package state

import (
	"fmt"
	"github.com/jpillora/longestcommon"
	"keboola-as-code/src/local"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// RenamePlan creates a plan for renaming objects that do not match the naming
func (s *State) RenamePlan() (plans []*local.RenamePlan) {
	for _, object := range s.All() {
		plan := &local.RenamePlan{}

		// The parent object may have already been renamed, so update first old state
		object.UpdateManifest(s.manifest, false)
		oldPathRel := object.RelativePath()
		plan.OldPath = filepath.Join(s.manifest.ProjectDir, oldPathRel)

		// Rename
		object.UpdateManifest(s.manifest, true)
		newPathRel := object.RelativePath()
		plan.NewPath = filepath.Join(s.manifest.ProjectDir, newPathRel)

		// Should be renamed?
		if plan.OldPath != plan.NewPath {
			// Get common prefix of the old and new path
			prefix := longestcommon.Prefix([]string{oldPathRel, newPathRel})
			// Remove from the prefix everything after the last separator
			prefix = regexp.
				MustCompile(fmt.Sprintf(`(^|%c)[^%c]*$`, os.PathSeparator, os.PathSeparator)).
				ReplaceAllString(prefix, "$1")
			// Generate description for logs
			if prefix != "" {
				plan.Description = fmt.Sprintf(
					`%s{%s -> %s}`,
					prefix,
					strings.TrimPrefix(oldPathRel, prefix),
					strings.TrimPrefix(newPathRel, prefix),
				)
			} else {
				plan.Description = fmt.Sprintf(`%s -> %s`, oldPathRel, newPathRel)
			}

			// Add to plan
			s.manifest.PersistRecord(object.Manifest())
			plans = append(plans, plan)
		}
	}

	return plans
}
