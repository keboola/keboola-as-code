package branchmetadata

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type mapper struct {
	state *state.State
}

func NewMapper(state *state.State) *mapper {
	return &mapper{state: state}
}

// MapAfterLocalLoad - load metadata from manifest to branch.
func (m *mapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
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
		if err != nil && !strings.HasPrefix(err.Error(), "missing project description file") {
			return err
		}
		if err == nil && file.Content != "" {
			branch.Metadata[model.ProjectDescriptionMetaKey] = strings.TrimRight(file.Content, " \r\n\t")
		}
	}

	return nil
}
