package aferofs

import (
	"github.com/spf13/afero"
	"go.nhat.io/aferocopy/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const CopyBufferSize uint = 512 * 1024 // 512 kB

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
		aferoSrc = fs.Backend()
	} else {
		return errors.Errorf(`unexpected type of src filesystem "%T"`, srcFs)
	}

	// Detect dst filesystem
	var aferoDst afero.Fs
	if dstFs == nil {
		// If nil, use OS filesystem
		aferoDst = &afero.Afero{Fs: afero.NewOsFs()}
	} else if fs, ok := dstFs.(*Fs); ok {
		// If filesystem implemented by Afero lib -> get lib backend
		aferoDst = fs.Backend()
	} else {
		return errors.Errorf(`unexpected type of dst filesystem "%T"`, dstFs)
	}

	// nolint: forbidigo
	return aferocopy.Copy(srcPath, dstPath, aferocopy.Options{
		SrcFs:          aferoSrc,
		DestFs:         aferoDst,
		Sync:           false,
		CopyBufferSize: CopyBufferSize,
		OnDirExists: func(srcFs afero.Fs, src string, destFs afero.Fs, dest string) aferocopy.DirExistsAction {
			return aferocopy.Replace
		},
	})
}
