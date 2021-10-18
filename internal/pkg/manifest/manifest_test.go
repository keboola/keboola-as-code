package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

type test struct {
	name string
	json string
	data *Content
}

func cases() []test {
	return []test{
		{
			name: `minimal`,
			json: minimalJson(),
			data: minimalStruct(),
		},
		{
			name: `full`,
			json: fullJson(),
			data: fullStruct(),
		},
	}
}

func TestNewManifest(t *testing.T) {
	t.Parallel()
	manifest := newTestManifest(t)
	assert.NotNil(t, manifest)
}

func TestManifestLoadNotFound(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)

	// Load
	manifest, err := LoadManifest(fs)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" not found`, err.Error())
}

func TestManifestLoad(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
		assert.NoError(t, err)

		// Write file
		path := filesystem.Join(filesystem.MetadataDir, FileName)
		assert.NoError(t, fs.WriteFile(filesystem.CreateFile(path, c.json)))

		// Load
		manifest, err := LoadManifest(fs)
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert naming (without internal fields)
		assert.Equal(t, c.data.Naming.Branch, manifest.Content.Naming.Branch, c.name)
		assert.Equal(t, c.data.Naming.Config, manifest.Content.Naming.Config, c.name)
		assert.Equal(t, c.data.Naming.ConfigRow, manifest.Content.Naming.ConfigRow, c.name)

		// Assert
		c.data.Naming = model.DefaultNaming()
		manifest.Naming = model.DefaultNaming()
		assert.Equal(t, c.data, manifest.Content, c.name)
	}
}

func TestManifestSave(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		// Create
		m := newTestManifest(t)
		m.AllowedBranches = c.data.AllowedBranches
		m.IgnoredComponents = c.data.IgnoredComponents
		m.Project.Id = c.data.Project.Id
		for _, branch := range c.data.Branches {
			assert.NoError(t, m.TrackRecord(branch))
		}
		for _, config := range c.data.Configs {
			assert.NoError(t, m.TrackRecord(config.ConfigManifest))
			for _, row := range config.Rows {
				assert.NoError(t, m.TrackRecord(row))
			}
		}

		// Save
		assert.NoError(t, m.Save())

		// Load file
		path := filesystem.Join(filesystem.MetadataDir, FileName)
		file, err := m.fs.ReadFile(path, "")
		assert.NoError(t, err)
		assert.Equal(t, testhelper.EscapeWhitespaces(c.json), testhelper.EscapeWhitespaces(file.Content), c.name)
	}
}

func TestManifestValidateEmpty(t *testing.T) {
	t.Parallel()
	m := &Manifest{Content: &Content{}}
	err := m.validate()
	assert.NotNil(t, err)
	expected := `manifest is not valid:
	- key="version", value="0", failed "required" validation
	- key="project.id", value="0", failed "required" validation
	- key="project.apiHost", value="", failed "required" validation
	- key="sortBy", value="", failed "oneof" validation
	- key="naming.branch", value="", failed "required" validation
	- key="naming.config", value="", failed "required" validation
	- key="naming.configRow", value="", failed "required" validation
	- key="naming.sharedCodeConfig", value="", failed "required" validation
	- key="naming.sharedCodeConfigRow", value="", failed "required" validation
	- key="naming.variables", value="", failed "required" validation
	- key="naming.variablesValues", value="", failed "required" validation
	- key="allowedBranches", value="[]", failed "required" validation`
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateMinimal(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	m := newManifest(0, "", fs)
	m.Content = minimalStruct()
	assert.NoError(t, m.validate())
}

func TestManifestValidateFull(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	m := newManifest(0, "", fs)
	m.Content = fullStruct()
	assert.NoError(t, m.validate())
}

func TestManifestValidateBadVersion(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	m := newManifest(0, "", fs)
	m.Content = minimalStruct()
	m.Version = 123
	err = m.validate()
	assert.Error(t, err)
	expected := "manifest is not valid:\n\t- key=\"version\", value=\"123\", failed \"max\" validation"
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateNestedField(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	m := newManifest(1, "connection.keboola.com", fs)
	m.Content = minimalStruct()
	m.Content.Branches = append(m.Content.Branches, &model.BranchManifest{
		BranchKey: model.BranchKey{Id: 0},
		Paths: model.Paths{
			PathInProject: model.PathInProject{
				ObjectPath: "foo",
				ParentPath: "bar",
			},
		},
	})
	err = m.validate()
	assert.Error(t, err)
	expected := "manifest is not valid:\n\t- key=\"branches[0].id\", value=\"0\", failed \"required\" validation"
	assert.Equal(t, expected, err.Error())
}

func TestIsObjectIgnored(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	m := newManifest(1, "connection.keboola.com", fs)
	m.Content = minimalStruct()
	m.Content.AllowedBranches = model.AllowedBranches{"dev-*", "123", "abc"}
	m.Content.IgnoredComponents = model.ComponentIds{"aaa", "bbb"}

	assert.False(t, m.IsObjectIgnored(
		&model.Branch{BranchKey: model.BranchKey{Id: 789}, Name: "dev-1"}),
	)
	assert.False(t, m.IsObjectIgnored(
		&model.Branch{BranchKey: model.BranchKey{Id: 123}, Name: "xyz"}),
	)
	assert.False(t, m.IsObjectIgnored(
		&model.Branch{BranchKey: model.BranchKey{Id: 789}, Name: "abc"}),
	)
	assert.True(t, m.IsObjectIgnored(
		&model.Branch{BranchKey: model.BranchKey{Id: 789}, Name: "xyz"}),
	)
	assert.True(t, m.IsObjectIgnored(
		&model.Config{ConfigKey: model.ConfigKey{ComponentId: "aaa"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&model.Config{ConfigKey: model.ConfigKey{ComponentId: "bbb"}}),
	)
	assert.False(t, m.IsObjectIgnored(
		&model.Config{ConfigKey: model.ConfigKey{ComponentId: "ccc"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&model.ConfigRow{ConfigRowKey: model.ConfigRowKey{ComponentId: "aaa"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&model.ConfigRow{ConfigRowKey: model.ConfigRowKey{ComponentId: "bbb"}}),
	)
	assert.False(t, m.IsObjectIgnored(
		&model.ConfigRow{ConfigRowKey: model.ConfigRowKey{ComponentId: "ccc"}}),
	)
}

func TestManifestRecordGetParent(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	m := newManifest(0, "", fs)
	branchManifest := &model.BranchManifest{BranchKey: model.BranchKey{Id: 123}}
	configManifest := &model.ConfigManifest{ConfigKey: model.ConfigKey{
		BranchId:    123,
		ComponentId: "keboola.foo",
		Id:          "456",
	}}
	assert.NoError(t, m.TrackRecord(branchManifest))
	parent, err := m.GetParent(configManifest)
	assert.Equal(t, branchManifest, parent)
	assert.NoError(t, err)
}

func TestManifestRecordGetParentNotFound(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	m := newManifest(0, "", fs)
	configManifest := &model.ConfigManifest{ConfigKey: model.ConfigKey{
		BranchId:    123,
		ComponentId: "keboola.foo",
		Id:          "456",
	}}
	parent, err := m.GetParent(configManifest)
	assert.Nil(t, parent)
	assert.Error(t, err)
	assert.Equal(t, `manifest record for branch "123" not found, referenced from config "branch:123/component:keboola.foo/config:456"`, err.Error())
}

func TestManifestRecordGetParentNil(t *testing.T) {
	t.Parallel()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	m := newManifest(0, "", fs)
	parent, err := m.GetParent(&model.BranchManifest{})
	assert.Nil(t, parent)
	assert.NoError(t, err)
}

func minimalJson() string {
	return `{
  "version": 1,
  "project": {
    "id": 12345,
    "apiHost": "foo.bar"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "rows/{config_row_id}-{config_row_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_id}-{config_row_name}",
    "variables": "variables",
    "variablesValues": "values/{config_row_name}"
  },
  "allowedBranches": [
    "*"
  ],
  "ignoredComponents": [
    "keboola.scheduler"
  ],
  "branches": [],
  "configurations": []
}
`
}

func minimalStruct() *Content {
	return &Content{
		Version: 1,
		Project: model.Project{
			Id:      12345,
			ApiHost: "foo.bar",
		},
		SortBy:   model.SortById,
		Naming:   model.DefaultNaming(),
		Filter:   model.DefaultFilter(),
		Branches: make([]*model.BranchManifest, 0),
		Configs:  make([]*model.ConfigManifestWithRows, 0),
	}
}

func fullJson() string {
	return `{
  "version": 1,
  "project": {
    "id": 12345,
    "apiHost": "foo.bar"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "rows/{config_row_id}-{config_row_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_id}-{config_row_name}",
    "variables": "variables",
    "variablesValues": "values/{config_row_name}"
  },
  "allowedBranches": [
    "foo",
    "bar"
  ],
  "ignoredComponents": [
    "abc"
  ],
  "branches": [
    {
      "id": 10,
      "path": "main"
    },
    {
      "id": 11,
      "path": "11-dev"
    }
  ],
  "configurations": [
    {
      "branchId": 10,
      "componentId": "keboola.ex-db-oracle",
      "id": "11",
      "path": "11-raw-data",
      "rows": [
        {
          "id": "101",
          "path": "rows/101-region-1"
        },
        {
          "id": "102",
          "path": "rows/102-region-2"
        }
      ]
    },
    {
      "branchId": 11,
      "componentId": "keboola.wr-db-mysql",
      "id": "12",
      "path": "12-current-month",
      "rows": [
        {
          "id": "103",
          "path": "rows/103-all"
        },
        {
          "id": "104",
          "path": "rows/104-sum"
        }
      ]
    }
  ]
}
`
}

func fullStruct() *Content {
	return &Content{
		Version: 1,
		Project: model.Project{
			Id:      12345,
			ApiHost: "foo.bar",
		},
		SortBy: model.SortById,
		Naming: model.DefaultNaming(),
		Filter: model.Filter{
			AllowedBranches:   model.AllowedBranches{"foo", "bar"},
			IgnoredComponents: model.ComponentIds{"abc"},
		},
		Branches: []*model.BranchManifest{
			{
				RecordState: model.RecordState{
					Persisted: true,
				},
				BranchKey: model.BranchKey{
					Id: 10,
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "main",
						ParentPath: "",
					},
				},
			},
			{
				RecordState: model.RecordState{
					Persisted: true,
				},
				BranchKey: model.BranchKey{
					Id: 11,
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "11-dev",
						ParentPath: "",
					},
				},
			},
		},
		Configs: []*model.ConfigManifestWithRows{
			{
				ConfigManifest: &model.ConfigManifest{
					RecordState: model.RecordState{
						Persisted: true,
					},
					ConfigKey: model.ConfigKey{
						BranchId:    10,
						ComponentId: "keboola.ex-db-oracle",
						Id:          "11",
					},
					Paths: model.Paths{
						PathInProject: model.PathInProject{
							ObjectPath: "11-raw-data",
							ParentPath: "main",
						},
					},
				},
				Rows: []*model.ConfigRowManifest{
					{
						RecordState: model.RecordState{
							Persisted: true,
						},
						ConfigRowKey: model.ConfigRowKey{
							Id:          "101",
							BranchId:    10,
							ComponentId: "keboola.ex-db-oracle",
							ConfigId:    "11",
						},
						Paths: model.Paths{
							PathInProject: model.PathInProject{
								ObjectPath: "rows/101-region-1",
								ParentPath: "main/11-raw-data",
							},
						},
					},
					{
						RecordState: model.RecordState{
							Persisted: true,
						},
						ConfigRowKey: model.ConfigRowKey{
							Id:          "102",
							BranchId:    10,
							ComponentId: "keboola.ex-db-oracle",
							ConfigId:    "11",
						},
						Paths: model.Paths{
							PathInProject: model.PathInProject{
								ObjectPath: "rows/102-region-2",
								ParentPath: "main/11-raw-data",
							},
						},
					},
				},
			},
			{
				ConfigManifest: &model.ConfigManifest{
					RecordState: model.RecordState{
						Persisted: true,
					},
					ConfigKey: model.ConfigKey{
						BranchId:    11,
						ComponentId: "keboola.wr-db-mysql",
						Id:          "12",
					},
					Paths: model.Paths{
						PathInProject: model.PathInProject{
							ObjectPath: "12-current-month",
							ParentPath: "11-dev",
						},
					},
				},
				Rows: []*model.ConfigRowManifest{
					{
						RecordState: model.RecordState{
							Persisted: true,
						},
						ConfigRowKey: model.ConfigRowKey{
							Id:          "103",
							BranchId:    11,
							ComponentId: "keboola.wr-db-mysql",
							ConfigId:    "12",
						},
						Paths: model.Paths{
							PathInProject: model.PathInProject{
								ObjectPath: "rows/103-all",
								ParentPath: "11-dev/12-current-month",
							},
						},
					},
					{
						RecordState: model.RecordState{
							Persisted: true,
						},
						ConfigRowKey: model.ConfigRowKey{
							Id:          "104",
							BranchId:    11,
							ComponentId: "keboola.wr-db-mysql",
							ConfigId:    "12",
						},
						Paths: model.Paths{
							PathInProject: model.PathInProject{
								ObjectPath: "rows/104-sum",
								ParentPath: "11-dev/12-current-month",
							},
						},
					},
				},
			},
		},
	}
}

func newTestManifest(t *testing.T) *Manifest {
	t.Helper()
	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), "")
	assert.NoError(t, err)
	manifest, err := NewManifest(123, "foo.bar", fs)
	assert.NoError(t, err)
	return manifest
}
