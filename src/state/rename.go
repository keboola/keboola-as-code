package state

import (
	"fmt"
	"github.com/jpillora/longestcommon"
	"keboola-as-code/src/local"
	"os"
	"regexp"
	"strings"
)

// RenamePlan creates a plan for renaming objects that do not match the naming
func (s *State) RenamePlan() (plans []*local.RenamePlan) {
	for _, object := range s.All() {
		plan := &local.RenamePlan{}

		// The parent object may have already been renamed, so update first old state
		object.UpdateManifest(s.manifest, false)
		plan.OldPath = object.RelativePath()

		// Rename
		object.UpdateManifest(s.manifest, true)
		plan.NewPath = object.RelativePath()

		// Should be renamed?
		if plan.OldPath != plan.NewPath {
			// Get common prefix of the old and new path
			prefix := longestcommon.Prefix([]string{plan.OldPath, plan.NewPath})
			// Remove from the prefix everything after the last separator
			prefix = regexp.
				MustCompile(fmt.Sprintf(`(^|%c)[^%c]*$`, os.PathSeparator, os.PathSeparator)).
				ReplaceAllString(prefix, "$1")
			// Generate description for logs
			if prefix != "" {
				plan.Description = fmt.Sprintf(
					`%s{%s -> %s}`,
					prefix, strings.TrimPrefix(plan.OldPath, prefix),
					strings.TrimPrefix(plan.NewPath, prefix),
				)
			} else {
				plan.Description = fmt.Sprintf(`%s -> %s`, plan.OldPath, plan.NewPath)
			}

			// Add to results
			plans = append(plans, plan)
		}
	}

	return plans
}
