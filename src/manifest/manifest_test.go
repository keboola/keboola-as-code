package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

type test struct {
	json string
	data *Content
}

var cases = []test{
	{
		json: minimalJson(),
		data: minimalStruct(),
	},
	{
		json: fullJson(),
		data: fullStruct(),
	},
}

func TestNewManifest(t *testing.T) {
	manifest, err := NewManifest(123, "connection.keboola.com", "foo", "bra")
	assert.NoError(t, err)
	assert.NotNil(t, manifest)
}

func TestManifestLoadNotFound(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, MetadataDir)
	assert.NoError(t, os.MkdirAll(metadataDir, 0755))

	// Load
	manifest, err := LoadManifest(projectDir, metadataDir)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" not found`, err.Error())
}

func TestManifestLoad(t *testing.T) {
	for _, c := range cases {
		projectDir := t.TempDir()
		metadataDir := filepath.Join(projectDir, MetadataDir)
		assert.NoError(t, os.MkdirAll(metadataDir, 0755))
		path := filepath.Join(metadataDir, FileName)

		// Write file
		assert.NoError(t, os.WriteFile(path, []byte(c.json), 0644))

		// Load
		manifest, err := LoadManifest(projectDir, metadataDir)
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert
		assert.Equal(t, c.data, manifest.Content)
	}
}

func TestManifestSave(t *testing.T) {
	for _, c := range cases {
		projectDir := t.TempDir()
		metadataDir := filepath.Join(projectDir, MetadataDir)
		assert.NoError(t, os.MkdirAll(metadataDir, 0755))
		path := filepath.Join(metadataDir, FileName)

		// Create
		m := newManifest(c.data.Version, c.data.Project.ApiHost, projectDir, metadataDir)
		m.AllowedBranches = c.data.AllowedBranches
		m.IgnoredComponents = c.data.IgnoredComponents
		m.Project.Id = c.data.Project.Id
		for _, branch := range c.data.Branches {
			m.TrackRecord(branch)
		}
		for _, config := range c.data.Configs {
			m.TrackRecord(config.ConfigManifest)
			for _, row := range config.Rows {
				m.TrackRecord(row)
			}
		}

		// Save
		assert.NoError(t, m.Save())

		// Load file
		file, err := os.ReadFile(path)
		assert.NoError(t, err)
		assert.Equal(t, utils.EscapeWhitespaces(c.json), utils.EscapeWhitespaces(string(file)))
	}
}

func TestManifestValidateEmpty(t *testing.T) {
	m := &Manifest{ProjectDir: "foo", MetadataDir: "bar", Content: &Content{}}
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
	- key="allowedBranches", value="[]", failed "required" validation`
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateMinimal(t *testing.T) {
	m := newManifest(0, "", "foo", "bar")
	m.Content = minimalStruct()
	err := m.validate()
	assert.Nil(t, err)
}

func TestManifestValidateFull(t *testing.T) {
	m := newManifest(0, "", "foo", "bar")
	m.Content = fullStruct()
	err := m.validate()
	assert.Nil(t, err)
}

func TestManifestValidateBadVersion(t *testing.T) {
	m := newManifest(0, "", "foo", "bar")
	m.Content = minimalStruct()
	m.Version = 123
	err := m.validate()
	assert.NotNil(t, err)
	expected := "manifest is not valid:\n\t- key=\"version\", value=\"123\", failed \"max\" validation"
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateNestedField(t *testing.T) {
	m := newManifest(1, "connection.keboola.com", "foo", "bar")
	m.Content = minimalStruct()
	m.Content.Branches = append(m.Content.Branches, &model.BranchManifest{
		BranchKey: model.BranchKey{Id: 0},
		Paths: model.Paths{
			Path:       "foo",
			ParentPath: "bar",
		},
	})
	err := m.validate()
	assert.NotNil(t, err)
	expected := "manifest is not valid:\n\t- key=\"branches[0].id\", value=\"0\", failed \"required\" validation"
	assert.Equal(t, expected, err.Error())
}

func TestIsObjectIgnored(t *testing.T) {
	m := newManifest(1, "connection.keboola.com", "foo", "bar")
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

func minimalJson() string {
	return `{
  "version": 1,
  "project": {
    "id": 12345,
    "apiHost": "connection.keboola.com"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "rows/{config_row_id}-{config_row_name}"
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
			ApiHost: "connection.keboola.com",
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
    "apiHost": "connection.keboola.com"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "rows/{config_row_id}-{config_row_name}"
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
			ApiHost: "connection.keboola.com",
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
					Path:       "main",
					ParentPath: "",
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
					Path:       "11-dev",
					ParentPath: "",
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
						Path:       "11-raw-data",
						ParentPath: "main",
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
							Path:       "rows/101-region-1",
							ParentPath: "main/11-raw-data",
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
							Path:       "rows/102-region-2",
							ParentPath: "main/11-raw-data",
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
						Path:       "12-current-month",
						ParentPath: "11-dev",
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
							Path:       "rows/103-all",
							ParentPath: "11-dev/12-current-month",
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
							Path:       "rows/104-sum",
							ParentPath: "11-dev/12-current-month",
						},
					},
				},
			},
		},
	}
}
