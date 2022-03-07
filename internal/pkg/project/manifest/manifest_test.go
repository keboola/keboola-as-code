package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type test struct {
	name    string
	json    string
	naming  naming.Template
	filter  model.ObjectsFilter
	records []model.ObjectManifest
}

func cases() []test {
	return []test{
		{
			name:    `minimal`,
			json:    minimalJson(),
			naming:  naming.TemplateWithIds(),
			filter:  model.NoFilter(),
			records: minimalRecords(),
		},
		{
			name:   `full`,
			json:   fullJson(),
			naming: naming.TemplateWithoutIds(),
			filter: model.NewFilter(
				model.AllowedBranches{"foo", "bar"},
				model.ComponentIds{"abc"},
			),
			records: fullRecords(),
		},
	}
}

func TestNewManifest(t *testing.T) {
	t.Parallel()
	m := New(123, `foo.bar`)
	assert.NotNil(t, m)
	assert.Equal(t, 123, m.project.Id)
	assert.Equal(t, `foo.bar`, m.project.ApiHost)
}

func TestManifestLoadNotFound(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Load
	manifest, err := Load(fs, false)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" not found`, err.Error())
}

func TestLoadManifestFile(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testfs.NewMemoryFs()

		// Write file
		path := Path()
		assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(path, c.json)))

		// Load
		manifest, err := Load(fs, false)
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert
		assert.Equal(t, c.naming, manifest.NamingTemplate(), c.name)
		assert.Equal(t, c.filter.AllowedBranches(), manifest.AllowedBranches(), c.name)
		assert.Equal(t, c.filter.IgnoredComponents(), manifest.IgnoredComponents(), c.name)
		assert.Equal(t, c.records, manifest.records.All(), c.name)
	}
}

func TestSaveManifestFile(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testfs.NewMemoryFs()

		// Save
		manifest := New(12345, "foo.bar")
		manifest.SetNamingTemplate(c.naming)
		manifest.SetAllowedBranches(c.filter.AllowedBranches())
		manifest.SetIgnoredComponents(c.filter.IgnoredComponents())
		assert.NoError(t, manifest.records.SetRecords(c.records))
		assert.NoError(t, manifest.Save(fs))

		// Load file
		file, err := fs.ReadFile(filesystem.NewFileDef(Path()))
		assert.NoError(t, err)
		assert.Equal(t, testhelper.EscapeWhitespaces(c.json), testhelper.EscapeWhitespaces(file.Content), c.name)
	}
}

func TestManifestValidateEmpty(t *testing.T) {
	t.Parallel()
	content := &file{}
	err := content.validate()
	assert.NotNil(t, err)
	expected := `manifest is not valid:
  - version is a required field
  - project.id is a required field
  - project.apiHost is a required field
  - sortBy must be one of [id path]
  - naming.branch is a required field
  - naming.config is a required field
  - naming.configRow is a required field
  - naming.schedulerConfig is a required field
  - naming.sharedCodeConfig is a required field
  - naming.sharedCodeConfigRow is a required field
  - naming.variablesConfig is a required field
  - naming.variablesValuesRow is a required field
  - allowedBranches is a required field`
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateMinimal(t *testing.T) {
	t.Parallel()
	content := newFile(12345, "foo.bar")
	content.setRecords(minimalRecords())
	assert.NoError(t, content.validate())
}

func TestManifestValidateFull(t *testing.T) {
	t.Parallel()
	content := newFile(12345, "foo.bar")
	content.setRecords(fullRecords())
	assert.NoError(t, content.validate())
}

func TestManifestValidateBadVersion(t *testing.T) {
	t.Parallel()
	content := newFile(12345, "foo.bar")
	content.setRecords(minimalRecords())
	content.Version = 123
	err := content.validate()
	assert.Error(t, err)
	expected := "manifest is not valid: version must be 2 or less"
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateNestedField(t *testing.T) {
	t.Parallel()
	content := newFile(12345, "foo.bar")
	content.setRecords(minimalRecords())
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
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(path, cyclicDependencyJson())))

	// Load
	manifest, err := Load(fs, false)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, "invalid manifest:\n  - a cyclic relation was found when resolving path to config \"branch:123/component:keboola.variables/config:111\"", err.Error())
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
  "templates": {
    "repositories": [
      {
        "type": "git",
        "name": "keboola",
        "url": "https://github.com/keboola/keboola-as-code-templates.git",
        "ref": "main"
      }
    ]
  },
  "branches": [],
  "configurations": []
}
`
}

func minimalRecords() []model.ObjectManifest {
	return []model.ObjectManifest{}
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
    "branch": "{branch_name}",
    "config": "{component_type}/{component_id}/{config_name}",
    "configRow": "rows/{config_row_name}",
    "schedulerConfig": "schedules/{config_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_name}",
    "variablesConfig": "variables",
    "variablesValuesRow": "values/{config_row_name}"
  },
  "allowedBranches": [
    "foo",
    "bar"
  ],
  "ignoredComponents": [
    "abc"
  ],
  "templates": {
    "repositories": [
      {
        "type": "git",
        "name": "keboola",
        "url": "https://github.com/keboola/keboola-as-code-templates.git",
        "ref": "main"
      }
    ]
  },
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

func fullRecords() []model.ObjectManifest {
	return []model.ObjectManifest{
		&model.BranchManifest{
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
		&model.BranchManifest{
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

		&model.ConfigManifest{
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
		&model.ConfigManifest{
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
		&model.ConfigManifest{
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
		&model.ConfigRowManifest{
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
		&model.ConfigRowManifest{
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
		&model.ConfigRowManifest{
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
		&model.ConfigRowManifest{
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
		&model.ConfigRowManifest{
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
		&model.ConfigRowManifest{
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
