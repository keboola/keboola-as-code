package load

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

func Run(fs filesystem.Fs) (*template.Inputs, error) {
	return template.LoadInputs(fs)
}
