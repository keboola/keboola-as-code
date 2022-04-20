package fsimporter

import (
	"github.com/google/go-jsonnet"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type Importer struct {
	root filesystem.Fs
}

func New(root filesystem.Fs) jsonnet.Importer {
	return &Importer{root: root}
}

func (i Importer) Import(importedFrom, filePath string) (contents jsonnet.Contents, foundAt string, err error) {
	// Make path absolute
	if !filesystem.IsAbs(filePath) {
		filePath = filesystem.Join(filesystem.Dir(importedFrom), filePath)
	}

	// Load file
	file, err := i.root.ReadFile(filesystem.NewFileDef(filePath))
	if err != nil {
		return jsonnet.Contents{}, "", err
	}

	// Return file content
	return jsonnet.MakeContents(file.Content), file.Path(), nil
}
