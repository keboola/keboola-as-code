package jsonnet

import (
	"fmt"

	"github.com/google/go-jsonnet"
)

type NopImporter struct{}

func NewNopImporter() jsonnet.Importer {
	return NopImporter{}
}

func (i NopImporter) Import(_, _ string) (contents jsonnet.Contents, foundAt string, err error) {
	return jsonnet.Contents{}, "", fmt.Errorf("imports are not enabled")
}
