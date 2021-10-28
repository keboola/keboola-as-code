package aferofs

import (
	"fmt"

	"github.com/nhatthm/aferocopy"
	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

func CopyFs2Fs(srcFs filesystem.Fs, srcPath string, dstFs filesystem.Fs, dstPath string) error {
	srcPath = filesystem.FromSlash(srcPath)
	dstPath = filesystem.FromSlash(dstPath)

	// Detect src filesystem
	var aferoSrc afero.Fs
	if srcFs == nil {
		// If nil, use OS filesystem
		aferoSrc = &afero.Afero{Fs: afero.NewOsFs()}
	} else if fs, ok := srcFs.(*Fs); ok {
		// If filesystem implemented by Afero lib -> get lib backend
		aferoSrc = fs.AferoFs()
	} else {
		return fmt.Errorf(`unexpected type of src filesyste "%T"`, srcFs)
	}

	// Detect dst filesystem
	var aferoDst afero.Fs
	if dstFs == nil {
		// If nil, use OS filesystem
		aferoDst = &afero.Afero{Fs: afero.NewOsFs()}
	} else if fs, ok := dstFs.(*Fs); ok {
		// If filesystem implemented by Afero lib -> get lib backend
		aferoDst = fs.AferoFs()
	} else {
		return fmt.Errorf(`unexpected type of dst filesyste "%T"`, dstFs)
	}

	// nolint: forbidigo
	return aferocopy.Copy(srcPath, dstPath, aferocopy.Options{
		SrcFs:  aferoSrc,
		DestFs: aferoDst,
		Sync:   true,
		OnDirExists: func(srcFs afero.Fs, src string, destFs afero.Fs, dest string) aferocopy.DirExistsAction {
			return aferocopy.Replace
		},
	})
}
