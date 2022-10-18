package description

import (
	"context"
	"strings"

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

func (m *mapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	switch o := recipe.Object.(type) {
	case *model.Branch:
		o.Description = m.normalize(o.Description)
		desc, found := o.Metadata[model.ProjectDescriptionMetaKey]
		if found {
			o.Metadata[model.ProjectDescriptionMetaKey] = m.normalize(desc)
		}
	case *model.Config:
		o.Description = m.normalize(o.Description)
	case *model.ConfigRow:
		o.Description = m.normalize(o.Description)
	}
	return nil
}

func (m *mapper) normalize(str string) string {
	return strings.TrimRight(str, " \r\n\t")
}
