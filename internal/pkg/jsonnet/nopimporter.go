package jsonnet

import (
	"github.com/google/go-jsonnet"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type NopImporter struct{}

func NewNopImporter() jsonnet.Importer {
	return NopImporter{}
}

func (i NopImporter) Import(_, _ string) (contents jsonnet.Contents, foundAt string, err error) {
	return jsonnet.Contents{}, "", errors.New("imports are not enabled")
}
