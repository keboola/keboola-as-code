package branchmetadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterLocalLoad - load metadata from manifest to branch.
func (m *branchMetadataMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	manifest, ok := recipe.ObjectManifest.(*model.BranchManifest)
	if !ok {
		return nil
	}

	branch, ok := recipe.Object.(*model.Branch)
	if !ok {
		return nil
	}

	branch.Metadata = manifest.MetadataMap()
	return nil
}
