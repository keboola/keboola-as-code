package manifest

import (
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
)

type test struct {
	name       string
	jsonNet    string
	records    []model.ObjectManifest
	mainConfig *model.ConfigKey
}

func cases() []test {
	return []test{
		{
			name:    `minimal`,
			jsonNet: minimalJsonNet(),
			records: minimalRecords(),
		},
		{
			name:    `full`,
			jsonNet: fullJsonNet(),
			records: fullRecords(),
			mainConfig: &model.ConfigKey{
				ComponentId: "keboola.ex-db-oracle",
				Id:          "config",
			},
		},
	}
}

func TestLoadManifestFile(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testfs.NewMemoryFs()

		// Write file
		path := Path()
		assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(path, c.jsonNet)))

		// Load
		manifestFile, err := Load(fs)
		assert.NotNil(t, manifestFile)
		assert.NoError(t, err)

		// Evaluate
		manifest, err := manifestFile.Evaluate(nil)
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert
		assert.Equal(t, c.records, manifest.records.All(), c.name)
		assert.Equal(t, c.mainConfig, manifest.MainConfig())
	}
}

func TestSaveManifestFile(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testfs.NewMemoryFs()

		// Save
		manifest := New()
		manifest.mainConfig = c.mainConfig
		assert.NoError(t, manifest.records.SetRecords(c.records))
		assert.NoError(t, manifest.Save(fs))

		// Load file
		file, err := fs.ReadFile(filesystem.NewFileDef(Path()))
		assert.NoError(t, err)
		assert.Equal(t, wildcards.EscapeWhitespaces(c.jsonNet), wildcards.EscapeWhitespaces(file.Content), c.name)
	}
}

func minimalJsonNet() string {
	return `{
  configurations: [],
}
`
}

func minimalRecords() []model.ObjectManifest {
	return []model.ObjectManifest{}
}

func fullJsonNet() string {
	return `{
  mainConfig: {
    componentId: "keboola.ex-db-oracle",
    id: "config",
  },
  configurations: [
    {
      componentId: "keboola.ex-db-oracle",
      id: "config",
      path: "config",
      rows: [
        {
          id: "row1",
          path: "rows/row1",
        },
        {
          id: "row2",
          path: "rows/row2",
        },
      ],
    },
  ],
}
`
}

func fullRecords() []model.ObjectManifest {
	return []model.ObjectManifest{
		&model.ConfigManifest{
			RecordState: model.RecordState{
				Persisted: true,
			},
			ConfigKey: model.ConfigKey{
				ComponentId: "keboola.ex-db-oracle",
				Id:          "config",
			},
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"",
					"config",
				),
			},
		},
		&model.ConfigRowManifest{
			RecordState: model.RecordState{
				Persisted: true,
			},
			ConfigRowKey: model.ConfigRowKey{
				Id:          "row1",
				ComponentId: "keboola.ex-db-oracle",
				ConfigId:    "config",
			},
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"config",
					"rows/row1",
				),
			},
		},
		&model.ConfigRowManifest{
			RecordState: model.RecordState{
				Persisted: true,
			},
			ConfigRowKey: model.ConfigRowKey{
				Id:          "row2",
				ComponentId: "keboola.ex-db-oracle",
				ConfigId:    "config",
			},
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"config",
					"rows/row2",
				),
			},
		},
	}
}
