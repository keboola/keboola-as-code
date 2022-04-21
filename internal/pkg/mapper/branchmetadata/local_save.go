package branchmetadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeLocalSave - store branch metadata to manifest.
func (m *branchMetadataMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	manifest, ok := recipe.ObjectManifest.(*model.BranchManifest)
	if !ok {
		return nil
	}

	branch, ok := recipe.Object.(*model.Branch)
	if !ok {
		return nil
	}

	manifest.Metadata = branch.MetadataOrderedMap()
	if len(manifest.Metadata.Keys()) == 0 {
		manifest.Metadata = nil
	}
	recipe.ChangedFields.Remove(`metadata`)
	return nil
}
