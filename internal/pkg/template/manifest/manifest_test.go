package manifest

import (
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
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
			jsonNet: minimalJSONNET(),
			records: minimalRecords(),
		},
		{
			name:    `full`,
			jsonNet: fullJSONNET(),
			records: fullRecords(),
			mainConfig: &model.ConfigKey{
				ComponentID: "keboola.ex-db-oracle",
				ID:          "config",
			},
		},
	}
}

func TestLoadManifestFile(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := aferofs.NewMemoryFs()

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
		fs := aferofs.NewMemoryFs()

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

func minimalJSONNET() string {
	return `{
  configurations: [],
}
`
}

func minimalRecords() []model.ObjectManifest {
	return []model.ObjectManifest{}
}

func fullJSONNET() string {
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
				ComponentID: "keboola.ex-db-oracle",
				ID:          "config",
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
				ID:          "row1",
				ComponentID: "keboola.ex-db-oracle",
				ConfigID:    "config",
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
				ID:          "row2",
				ComponentID: "keboola.ex-db-oracle",
				ConfigID:    "config",
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
