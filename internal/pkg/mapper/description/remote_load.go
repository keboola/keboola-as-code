package description

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// descriptionMapper normalize object description loaded from API.
// Description is normalized in the same way as when reading a local "description.md" file.
// The white characters at the end are removed.
type descriptionMapper struct{}

func NewMapper() *descriptionMapper {
	return &descriptionMapper{}
}

func (m *descriptionMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	switch o := recipe.Object.(type) {
	case *model.Branch:
		o.Description = m.normalize(o.Description)
	case *model.Config:
		o.Description = m.normalize(o.Description)
	case *model.ConfigRow:
		o.Description = m.normalize(o.Description)
	}
	return nil
}

func (m *descriptionMapper) normalize(str string) string {
	return strings.TrimRight(str, " \r\n\t")
}
