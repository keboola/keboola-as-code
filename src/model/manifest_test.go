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
	data *Manifest
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

func TestLoadNotFound(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, MetadataDir)
	assert.NoError(t, os.MkdirAll(metadataDir, 0650))

	// Load
	manifest, err := LoadManifest(projectDir, metadataDir)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" not found`, err.Error())
}

func TestLoad(t *testing.T) {
	for _, c := range cases {
		projectDir := t.TempDir()
		metadataDir := filepath.Join(projectDir, MetadataDir)
		assert.NoError(t, os.MkdirAll(metadataDir, 0650))
		path := filepath.Join(metadataDir, ManifestFileName)

		// Write file
		assert.NoError(t, os.WriteFile(path, []byte(c.json), 0600))

		// Load
		manifest, err := LoadManifest(projectDir, metadataDir)
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert
		manifest.ProjectDir = "foo"
		manifest.MetadataDir = "bar"
		manifest.Path = ""
		c.data.Path = ""
		assert.Equal(t, c.data, manifest)
	}
}

func TestSave(t *testing.T) {
	for _, c := range cases {
		projectDir := t.TempDir()
		metadataDir := filepath.Join(projectDir, MetadataDir)
		assert.NoError(t, os.MkdirAll(metadataDir, 0655))
		path := filepath.Join(metadataDir, ManifestFileName)

		// Save
		m := c.data
		m.ProjectDir = projectDir
		m.MetadataDir = metadataDir
		assert.NoError(t, m.Save())

		// Load file
		file, err := os.ReadFile(path)
		assert.NoError(t, err)
		assert.Equal(t, utils.EscapeWhitespaces(c.json), utils.EscapeWhitespaces(string(file)))
	}
}

func TestValidateEmpty(t *testing.T) {
	m := &Manifest{ProjectDir: "foo", MetadataDir: "bar"}
	err := m.validate()
	assert.NotNil(t, err)
	expected := `manifest is not valid: 
- key="version", value="0", failed "required" validation
- key="project", value="<nil>", failed "required" validation`
	assert.Equal(t, expected, err.Error())
}

func TestValidateMinimal(t *testing.T) {
	m := minimalStruct()
	err := m.validate()
	assert.Nil(t, err)
}

func TestValidateFull(t *testing.T) {
	m := fullStruct()
	err := m.validate()
	assert.Nil(t, err)
}

func TestValidateBadVersion(t *testing.T) {
	m := minimalStruct()
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
  "branches": [],
  "configurations": []
}`
}

func minimalStruct() *Manifest {
	return &Manifest{
		ProjectDir:  "foo",
		MetadataDir: "bar",
		Version:     1,
		Project: &ProjectManifest{
			Id:      12345,
			ApiHost: "keboola.connection.com",
		},
		Branches: make([]*BranchManifest, 0),
		Configs:  make([]*ConfigManifest, 0),
	}
}

func fullJson() string {
	return `{
  "version": 1,
  "project": {
    "id": 12345,
    "apiHost": "keboola.connection.com"
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
}`
}

func fullStruct() *Manifest {
	return &Manifest{
		ProjectDir:  "foo",
		MetadataDir: "bar",
		Version:     1,
		Project: &ProjectManifest{
			Id:      12345,
			ApiHost: "keboola.connection.com",
		},
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
		Configs: []*ConfigManifest{
			{
				ManifestPaths: ManifestPaths{
					Path:       "11-raw-data",
					ParentPath: "main",
				},
				BranchId:    10,
				ComponentId: "keboola.ex-db-oracle",
				Id:          "11",
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
				ManifestPaths: ManifestPaths{
					Path:       "12-current-month",
					ParentPath: "11-dev",
				},
				BranchId:    11,
				ComponentId: "keboola.wr-db-mysql",
				Id:          "12",
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
