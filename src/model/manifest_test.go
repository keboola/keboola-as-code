package model

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"testing"
)

type test struct {
	json string
	data *ManifestContent
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
		path := filepath.Join(metadataDir, ManifestFileName)

		// Write file
		assert.NoError(t, os.WriteFile(path, []byte(c.json), 0644))

		// Load
		manifest, err := LoadManifest(projectDir, metadataDir)
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert
		assert.Equal(t, c.data, manifest.ManifestContent)
	}
}

func TestManifestSave(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	for _, c := range cases {
		projectDir := t.TempDir()
		metadataDir := filepath.Join(projectDir, MetadataDir)
		assert.NoError(t, os.MkdirAll(metadataDir, 0755))
		path := filepath.Join(metadataDir, ManifestFileName)

		// Create
		m := newManifest(c.data.Version, c.data.Project.ApiHost, projectDir, metadataDir)
		m.Project.Id = c.data.Project.Id
		for _, branch := range c.data.Branches {
			assert.NoError(t, m.SaveModel(branch, utils.EmptyOrderedMap(), logger))
		}
		for _, config := range c.data.Configs {
			assert.NoError(t, m.SaveModel(config.ConfigManifest, utils.EmptyOrderedMap(), logger))
			for _, row := range config.Rows {
				assert.NoError(t, m.SaveModel(row, utils.EmptyOrderedMap(), logger))
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
	m := &Manifest{ProjectDir: "foo", MetadataDir: "bar", ManifestContent: &ManifestContent{}}
	err := m.validate()
	assert.NotNil(t, err)
	expected := `manifest is not valid: 
- key="version", value="0", failed "required" validation
- key="project", value="<nil>", failed "required" validation
- key="sortBy", value="", failed "oneof" validation
- key="naming", value="<nil>", failed "required" validation`
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateMinimal(t *testing.T) {
	m := newManifest(0, "", "foo", "bar")
	m.ManifestContent = minimalStruct()
	err := m.validate()
	assert.Nil(t, err)
}

func TestManifestValidateFull(t *testing.T) {
	m := newManifest(0, "", "foo", "bar")
	m.ManifestContent = fullStruct()
	err := m.validate()
	assert.Nil(t, err)
}

func TestManifestValidateBadVersion(t *testing.T) {
	m := newManifest(0, "", "foo", "bar")
	m.ManifestContent = minimalStruct()
	m.Version = 123
	err := m.validate()
	assert.NotNil(t, err)
	expected := `manifest is not valid: key="version", value="123", failed "max" validation`
	assert.Equal(t, expected, err.Error())
}

func minimalJson() string {
	return `{
  "version": 1,
  "project": {
    "id": 12345,
    "apiHost": "keboola.connection.com"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "{config_row_id}-{config_row_name}"
  },
  "branches": [],
  "configurations": []
}
`
}

func minimalStruct() *ManifestContent {
	return &ManifestContent{
		Version: 1,
		Project: &ProjectManifest{
			Id:      12345,
			ApiHost: "keboola.connection.com",
		},
		SortBy:   SortById,
		Naming:   DefaultNaming(),
		Branches: make([]*BranchManifest, 0),
		Configs:  make([]*ConfigManifestWithRows, 0),
	}
}

func fullJson() string {
	return `{
  "version": 1,
  "project": {
    "id": 12345,
    "apiHost": "keboola.connection.com"
  },
  "sortBy": "id",
  "naming": {
    "branch": "{branch_id}-{branch_name}",
    "config": "{component_type}/{component_id}/{config_id}-{config_name}",
    "configRow": "{config_row_id}-{config_row_name}"
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
          "path": "101-region-1"
        },
        {
          "id": "102",
          "path": "102-region-2"
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
          "path": "103-all"
        },
        {
          "id": "104",
          "path": "104-sum"
        }
      ]
    }
  ]
}
`
}

func fullStruct() *ManifestContent {
	return &ManifestContent{
		Version: 1,
		Project: &ProjectManifest{
			Id:      12345,
			ApiHost: "keboola.connection.com",
		},
		SortBy: SortById,
		Naming: DefaultNaming(),
		Branches: []*BranchManifest{
			{
				ManifestPaths: ManifestPaths{
					Path:       "main",
					ParentPath: "",
				},
				Id: 10,
			},
			{
				ManifestPaths: ManifestPaths{
					Path:       "11-dev",
					ParentPath: "",
				},
				Id: 11,
			},
		},
		Configs: []*ConfigManifestWithRows{
			{
				ConfigManifest: &ConfigManifest{
					ManifestPaths: ManifestPaths{
						Path:       "11-raw-data",
						ParentPath: "main",
					},
					BranchId:    10,
					ComponentId: "keboola.ex-db-oracle",
					Id:          "11",
				},
				Rows: []*ConfigRowManifest{
					{
						ManifestPaths: ManifestPaths{
							Path:       "101-region-1",
							ParentPath: "main/11-raw-data/rows",
						},
						Id:          "101",
						BranchId:    10,
						ComponentId: "keboola.ex-db-oracle",
						ConfigId:    "11",
					},
					{
						ManifestPaths: ManifestPaths{
							Path:       "102-region-2",
							ParentPath: "main/11-raw-data/rows",
						},
						Id:          "102",
						BranchId:    10,
						ComponentId: "keboola.ex-db-oracle",
						ConfigId:    "11",
					},
				},
			},
			{
				ConfigManifest: &ConfigManifest{
					ManifestPaths: ManifestPaths{
						Path:       "12-current-month",
						ParentPath: "11-dev",
					},
					BranchId:    11,
					ComponentId: "keboola.wr-db-mysql",
					Id:          "12",
				},
				Rows: []*ConfigRowManifest{
					{
						ManifestPaths: ManifestPaths{
							Path:       "103-all",
							ParentPath: "11-dev/12-current-month/rows",
						},
						Id:          "103",
						BranchId:    11,
						ComponentId: "keboola.wr-db-mysql",
						ConfigId:    "12",
					},
					{
						ManifestPaths: ManifestPaths{
							Path:       "104-sum",
							ParentPath: "11-dev/12-current-month/rows",
						},
						Id:          "104",
						BranchId:    11,
						ComponentId: "keboola.wr-db-mysql",
						ConfigId:    "12",
					},
				},
			},
		},
	}
}
