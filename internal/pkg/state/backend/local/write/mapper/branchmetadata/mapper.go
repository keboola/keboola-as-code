package branchmetadata

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type mapper struct {
	dependencies
}

type dependencies interface {
}

func NewMapper() *mapper {
	return &mapper{}
}

// MapBeforeLocalSave - store branch metadata to manifest.
func (m *mapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	manifest, ok := recipe.ObjectManifest.(*model.BranchManifest)
	if !ok {
		return nil
	}

	branch, ok := recipe.Object.(*model.Branch)
	if !ok {
		return nil
	}

	if branch.IsDefault {
		desc, found := branch.Metadata[model.ProjectDescriptionMetaKey]
		path := m.state.NamingGenerator().DescriptionFilePath(".")
		markdownFile := filesystem.NewRawFile(path, strings.TrimRight(desc, " \r\n\t")+"\n")
		recipe.Files.
			Add(markdownFile).
			AddTag(model.FileTypeMarkdown).
			AddTag(model.FileKindProjectDescription)
		if found {
			delete(branch.Metadata, model.ProjectDescriptionMetaKey)
		}
	}

	manifest.Metadata = branch.MetadataOrderedMap()
	if len(manifest.Metadata.Keys()) == 0 {
		manifest.Metadata = nil
	}
	recipe.ChangedFields.Remove(`metadata`)
	return nil
}
