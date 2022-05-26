package branchmetadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
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

	if branch.IsDefault {
		fileToLoad := recipe.Files.
			Load(m.state.NamingGenerator().DescriptionFilePath(".")).
			AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
			SetDescription("project description").
			AddTag(model.FileTypeMarkdown).
			AddTag(model.FileKindProjectDescription)
		file, err := fileToLoad.ReadFile()
		if err != nil {
			return err
		}
		if file.Content != "" {
			branch.Metadata[model.ProjectDescriptionMetaKey] = file.Content
		}
	}

	return nil
}
