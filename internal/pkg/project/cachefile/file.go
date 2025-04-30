package cachefile

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const FileName = "project.json"

func Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

type File struct {
	Backends        []string         `json:"backends"`
	Features        keboola.Features `json:"features"`
	DefaultBranchID keboola.BranchID `json:"defaultBranchId"`
}

func New() *File {
	return &File{
		Backends: make([]string, 0),
		Features: make(keboola.Features, 0),
	}
}

func Load(ctx context.Context, fs filesystem.Fs) (*File, error) {
	content := New()

	path := Path()
	if fs.IsFile(ctx, path) {
		if _, err := fs.FileLoader().ReadJSONFileTo(ctx, filesystem.NewFileDef(path).SetDescription("project backends/features"), content); err != nil {
			return nil, err
		}
	}
	return content, nil
}

func (f *File) Save(ctx context.Context, fs filesystem.Fs, backends []string, featuresMap keboola.FeaturesMap, branch keboola.BranchID) error {
	if len(backends) != 0 {
		f.Backends = backends
	}

	if len(featuresMap.ToSlice()) != 0 {
		f.Features = featuresMap.ToSlice()
	}

	f.DefaultBranchID = branch

	// Write JSON file
	content, err := json.EncodeString(f, true)
	if err != nil {
		return errors.PrefixError(err, "cannot encode manifest")
	}
	rawFile := filesystem.NewRawFile(Path(), content)
	if err := fs.WriteFile(ctx, rawFile); err != nil {
		return err
	}
	return nil
}
