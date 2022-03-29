package save

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/manifest"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	saveProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	saveTemplateManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/save"
)

type Dependencies interface {
	Logger() log.Logger
}

func Run(m manifest.Manifest, d Dependencies) (changed bool, err error) {
	switch v := m.(type) {
	case *projectManifest.Manifest:
		return saveProjectManifest.Run(v, d)
	case *templateManifest.Manifest:
		return saveTemplateManifest.Run(v, d)
	case *manifest.InMemory:
		// nop
		return false, nil
	default:
		panic(fmt.Errorf(`unexpected manifest type "%T"`, m))
	}
}
