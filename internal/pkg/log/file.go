package log

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type File struct {
	file *os.File
	path string
	temp bool
}

// NewLogFile creates a log file defined in the flags or create a temp file.
// Log file can be outside project directory, so it is NOT using virtual filesystem.
func NewLogFile(path string) (*File, error) {
	f := &File{}
	if len(path) == 0 {
		// Generate a unique hash if multiple instances start simultaneously
		randomHash := ``
		randomBytes := make([]byte, 6)
		if _, err := rand.Read(randomBytes); err == nil {
			randomHash = fmt.Sprintf(`-%x`, randomBytes)
		}

		// nolint forbidigo
		f.path = filepath.Join(os.TempDir(), fmt.Sprintf("keboola-as-code-%d%s.txt", time.Now().Unix(), randomHash))
		f.temp = true // temp log file will be removed. It will be preserved only in case of error
	} else {
		// nolint: forbidigo
		if v, err := filepath.Abs(path); err == nil {
			f.path = v
		} else {
			return nil, err
		}
		f.temp = false // log file defined by user will be preserved
	}

	// nolint: forbidigo
	if file, err := os.OpenFile(f.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600); err == nil {
		f.file = file
		return f, nil
	} else {
		return nil, err
	}
}

func (f *File) File() *os.File {
	return f.file
}

func (f *File) Path() string {
	return f.path
}

func (f *File) IsTemp() bool {
	return f.temp
}

func (f *File) TearDown(errorOccurred bool) {
	if f == nil {
		return
	}

	if err := f.file.Close(); err != nil {
		panic(errors.Errorf("cannot close log file \"%s\": %w", f.path, err))
	}

	// No error -> remove log file if temporary
	if !errorOccurred && f.temp {
		// nolint: forbidigo
		if err := os.Remove(f.path); err != nil {
			panic(errors.Errorf("cannot remove temp log file \"%s\": %w", f.path, err))
		}
	}
}
