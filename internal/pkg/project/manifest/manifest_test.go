package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
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
	m := New(123, `foo.bar`)
	assert.NotNil(t, m)
	assert.Equal(t, 123, m.content.Project.Id)
	assert.Equal(t, `foo.bar`, m.content.Project.ApiHost)
}

func TestManifestLoadNotFound(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Load
	manifest, err := Load(fs)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" not found`, err.Error())
}

func TestManifestLoad(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testfs.NewMemoryFs()

		// Write file
		path := filesystem.Join(filesystem.MetadataDir, FileName)
		assert.NoError(t, fs.WriteFile(filesystem.NewFile(path, c.json)))

		// Load
		manifest, err := Load(fs)
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert
		assert.Equal(t, c.data, manifest.content, c.name)
	}
}

func TestManifestSave(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testfs.NewMemoryFs()

		// Create
		m := New(c.data.Project.Id, `foo.bar`)
		m.content.AllowedBranches = c.data.AllowedBranches
		m.content.IgnoredComponents = c.data.IgnoredComponents
		assert.NoError(t, m.records.SetRecords(c.data.allRecords()))

		// Save
		assert.NoError(t, m.Save(fs))

		// Load file
		file, err := fs.ReadFile(Path(), "")
		assert.NoError(t, err)
		assert.Equal(t, testhelper.EscapeWhitespaces(c.json), testhelper.EscapeWhitespaces(file.Content), c.name)
	}
}

func TestManifestValidateEmpty(t *testing.T) {
	t.Parallel()
	content := &Content{}
	err := content.validate()
	assert.NotNil(t, err)
	expected := `manifest is not valid:
  - version is a required field
  - project.id is a required field
  - project.apiHost is a required field
  - project.sortBy must be one of [id path]
  - naming.branch is a required field
  - naming.config is a required field
  - naming.configRow is a required field
  - naming.schedulerConfig is a required field
  - naming.sharedCodeConfig is a required field
  - naming.sharedCodeConfigRow is a required field
  - naming.variablesConfig is a required field
  - naming.variablesValuesRow is a required field
  - naming.allowedBranches is a required field`
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateMinimal(t *testing.T) {
	t.Parallel()
	content := minimalStruct()
	assert.NoError(t, content.validate())
}

func TestManifestValidateFull(t *testing.T) {
	t.Parallel()
	content := fullStruct()
	assert.NoError(t, content.validate())
}

func TestManifestValidateBadVersion(t *testing.T) {
	t.Parallel()
	content := minimalStruct()
	content.Version = 123
	err := content.validate()
	assert.Error(t, err)
	expected := "manifest is not valid: version must be 2 or less"
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateNestedField(t *testing.T) {
	t.Parallel()
	content := minimalStruct()
	content.Branches = append(content.Branches, &model.BranchManifest{
		BranchKey: model.BranchKey{Id: 0},
		Paths: model.Paths{
			AbsPath: model.NewAbsPath(
				"bar",
				"foo",
			),
		},
	})
	err := content.validate()
	assert.Error(t, err)
	expected := "manifest is not valid: branches[0].id is a required field"
	assert.Equal(t, expected, err.Error())
}

func TestManifestCyclicDependency(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Write file
	path := filesystem.Join(filesystem.MetadataDir, FileName)
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(path, cyclicDependencyJson())))

	// Load
	manifest, err := Load(fs)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `cannot load manifest: a cyclic relation was found when resolving path to config "branch:123/component:keboola.variables/config:111"`, err.Error())
}

func minimalJson() string {
	return `{
  "version": 2,
  "project": {
    "id": 12345,
    "apiHost": "foo.bar"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "rows/{config_row_id}-{config_row_name}",
    "schedulerConfig": "schedules/{config_id}-{config_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_id}-{config_row_name}",
    "variablesConfig": "variables",
    "variablesValuesRow": "values/{config_row_id}-{config_row_name}"
  },
  "allowedBranches": [
    "*"
  ],
  "ignoredComponents": [],
  "branches": [],
  "configurations": []
}
`
}

func minimalStruct() *Content {
	return &Content{
		Version: 2,
		Project: Project{
			Id:      12345,
			ApiHost: "foo.bar",
		},
		SortBy:   model.SortById,
		Naming:   naming.TemplateWithIds(),
		Filter:   model.DefaultFilter(),
		Branches: make([]*model.BranchManifest, 0),
		Configs:  make([]*model.ConfigManifestWithRows, 0),
	}
}

func fullJson() string {
	return `{
  "version": 2,
  "project": {
    "id": 12345,
    "apiHost": "foo.bar"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "rows/{config_row_id}-{config_row_name}",
    "schedulerConfig": "schedules/{config_id}-{config_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_id}-{config_row_name}",
    "variablesConfig": "variables",
    "variablesValuesRow": "values/{config_row_id}-{config_row_name}"
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
      "componentId": "keboola.variables",
      "id": "13",
      "path": "variables",
      "relations": [
        {
          "componentId": "keboola.wr-db-mysql",
          "configId": "12",
          "type": "variablesFor"
        }
      ],
      "rows": [
        {
          "id": "105",
          "path": "values/default"
        },
        {
          "id": "106",
          "path": "values/other"
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
		Version: 2,
		Project: Project{
			Id:      12345,
			ApiHost: "foo.bar",
		},
		SortBy: model.SortById,
		Naming: naming.TemplateWithIds(),
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
					AbsPath: model.NewAbsPath(
						"",
						"main",
					),
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
					AbsPath: model.NewAbsPath(
						"",
						"11-dev",
					),
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
						AbsPath: model.NewAbsPath(
							"main",
							"11-raw-data",
						),
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
							AbsPath: model.NewAbsPath(
								"main/11-raw-data",
								"rows/101-region-1",
							),
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
							AbsPath: model.NewAbsPath(
								"main/11-raw-data",
								"rows/102-region-2",
							),
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
						ComponentId: "keboola.variables",
						Id:          "13",
					},
					Paths: model.Paths{
						AbsPath: model.NewAbsPath(
							"11-dev/12-current-month",
							"variables",
						),
					},
					Relations: model.Relations{
						&model.VariablesForRelation{
							ComponentId: "keboola.wr-db-mysql",
							ConfigId:    "12",
						},
					},
				},
				Rows: []*model.ConfigRowManifest{
					{
						RecordState: model.RecordState{
							Persisted: true,
						},
						ConfigRowKey: model.ConfigRowKey{
							Id:          "105",
							BranchId:    11,
							ComponentId: "keboola.variables",
							ConfigId:    "13",
						},
						Paths: model.Paths{
							AbsPath: model.NewAbsPath(
								"11-dev/12-current-month/variables",
								"values/default",
							),
						},
					},
					{
						RecordState: model.RecordState{
							Persisted: true,
						},
						ConfigRowKey: model.ConfigRowKey{
							Id:          "106",
							BranchId:    11,
							ComponentId: "keboola.variables",
							ConfigId:    "13",
						},
						Paths: model.Paths{
							AbsPath: model.NewAbsPath(
								"11-dev/12-current-month/variables",
								"values/other",
							),
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
						AbsPath: model.NewAbsPath(
							"11-dev",
							"12-current-month",
						),
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
							AbsPath: model.NewAbsPath(
								"11-dev/12-current-month",
								"rows/103-all",
							),
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
							AbsPath: model.NewAbsPath(
								"11-dev/12-current-month",
								"rows/104-sum",
							),
						},
					},
				},
			},
		},
	}
}

func cyclicDependencyJson() string {
	return `{
  "version": 2,
  "project": {
    "id": 12345,
    "apiHost": "foo.bar"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "rows/{config_row_id}-{config_row_name}",
    "schedulerConfig": "schedules/{config_id}-{config_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_id}-{config_row_name}"
  },
  "allowedBranches": [
    "*"
  ],
  "ignoredComponents": [],
  "branches": [
    {
      "id": 123,
      "path": "main"
    }
  ],
  "configurations": [
    {
      "branchId": 123,
      "componentId": "keboola.variables",
      "id": "111",
      "path": "variables",
      "relations": [
        {
          "componentId": "keboola.variables",
          "configId": "222",
          "type": "variablesFor"
        }
      ]
    },
    {
      "branchId": 123,
      "componentId": "keboola.variables",
      "id": "222",
      "path": "variables",
      "relations": [
        {
          "componentId": "keboola.variables",
          "configId": "111",
          "type": "variablesFor"
        }
      ]
    }
  ]
}
`
}
