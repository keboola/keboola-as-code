package save

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	saveProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	saveTemplateManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/save"
)

type Dependencies interface {
	Logger() log.Logger
}

func Run(m manifest.Manifest, d Dependencies) (changed bool, err error) {
	switch v := m.(type) {
	case *project.Manifest:
		return saveProjectManifest.Run(v, d)
	case *template.Manifest:
		return saveTemplateManifest.Run(v, d)
	default:
		panic(fmt.Errorf(`unexpected manifest type "%T"`, m))
	}
}
