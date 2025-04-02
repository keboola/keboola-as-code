package manifest

import (
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type test struct {
	name       string
	jsonnet    string
	records    []model.ObjectManifest
	mainConfig *model.ConfigKey
}

func cases() []test {
	return []test{
		{
			name:    `minimal`,
			jsonnet: minimalJsonnet(),
			records: minimalRecords(),
		},
		{
			name:    `full`,
			jsonnet: fullJsonnet(),
			records: fullRecords(),
			mainConfig: &model.ConfigKey{
				ComponentID: "keboola.ex-db-oracle",
				ID:          "config",
			},
		},
	}
}

func TestLoadMinimalManifestFile(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	for _, c := range cases() {
		if c.name != "minimal" {
			continue
		}

		fs := aferofs.NewMemoryFs()

		// Write file
		path := Path()
		require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(path, c.jsonnet)))

		// Load
		manifestFile, err := Load(ctx, fs)
		assert.NotNil(t, manifestFile)
		require.NoError(t, err)

		// Evaluate
		manifest, err := manifestFile.EvaluateAlwaysWithRecords(t.Context(), nil)
		assert.NotNil(t, manifest)
		require.NoError(t, err)

		// Assert
		assert.Equal(t, c.records, manifest.All(), c.name)
		assert.Equal(t, c.mainConfig, manifest.MainConfig())
	}
}

func TestLoadManifestFile(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	for _, c := range cases() {
		if c.name == "minimal" {
			continue
		}

		fs := aferofs.NewMemoryFs()

		// Write file
		path := Path()
		require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(path, c.jsonnet)))

		// Load
		manifestFile, err := Load(ctx, fs)
		assert.NotNil(t, manifestFile)
		require.NoError(t, err)

		// Evaluate
		manifest, err := manifestFile.Evaluate(t.Context(), nil)
		assert.NotNil(t, manifest)
		require.NoError(t, err)

		// Assert
		assert.Equal(t, c.records, manifest.All(), c.name)
		assert.Equal(t, c.mainConfig, manifest.MainConfig())
	}
}

func TestSaveManifestFile(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	for _, c := range cases() {
		fs := aferofs.NewMemoryFs()

		// Save
		manifest := New()
		manifest.mainConfig = c.mainConfig
		require.NoError(t, manifest.SetRecords(c.records))
		require.NoError(t, manifest.Save(ctx, fs))

		// Load file
		file, err := fs.ReadFile(ctx, filesystem.NewFileDef(Path()))
		require.NoError(t, err)
		assert.Equal(t, wildcards.EscapeWhitespaces(c.jsonnet), wildcards.EscapeWhitespaces(file.Content), c.name)
	}
}

func minimalJsonnet() string {
	return `{
  configurations: [],
}
`
}

func minimalRecords() []model.ObjectManifest {
	return []model.ObjectManifest{}
}

func fullJsonnet() string {
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
