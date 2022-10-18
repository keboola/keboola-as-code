package relations

import (
	"context"

	"github.com/keboola/go-utils/pkg/deepcopy"

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

// MapAfterLocalLoad - load relations from manifest to object.
func (m *mapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	manifest, ok := recipe.ObjectManifest.(model.ObjectManifestWithRelations)
	if !ok {
		return nil
	}

	object, ok := recipe.Object.(model.ObjectWithRelations)
	if !ok {
		return nil
	}

	object.SetRelations(deepcopy.Copy(manifest.GetRelations()).(model.Relations))
	return nil
}
