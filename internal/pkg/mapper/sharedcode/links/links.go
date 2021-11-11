package links

import (
	"regexp"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	IdFormat   = `{{<ID>}}`    // link to shared code used in API
	PathFormat = `{{:<PATH>}}` // link to shared code used locally
	IdRegexp   = `[0-9a-zA-Z_\-]+`
	PathRegexp = `[^:{}]+`
)

// mapper replaces "shared_code_id" with "shared_code_path" in local fs.
type mapper struct {
	model.MapperContext
	localManager *local.Manager
	idRegexp     *regexp.Regexp
	pathRegexp   *regexp.Regexp
}

func NewMapper(localManager *local.Manager, context model.MapperContext) *mapper {
	return &mapper{
		MapperContext: context,
		localManager:  localManager,
		idRegexp:      idRegexp(),
		pathRegexp:    pathRegexp(),
	}
}
