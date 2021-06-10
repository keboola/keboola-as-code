package manifest

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

var cases []test = []test{
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
	manifest, err := NewManifest(123, "connection.keboola.com")
	assert.NoError(t, err)
	assert.NotNil(t, manifest)
}

func TestLoad(t *testing.T) {
	for _, c := range cases {
		tempDir := t.TempDir()
		path := filepath.Join(tempDir, FileName)

		// Write file
		assert.NoError(t, os.WriteFile(path, []byte(c.json), 0600))

		// Load
		manifest, err := Load(tempDir)
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert
		manifest.path = ""
		c.data.path = ""
		assert.Equal(t, c.data, manifest)
	}
}

func TestSave(t *testing.T) {
	for _, c := range cases {
		tempDir := t.TempDir()
		path := filepath.Join(tempDir, FileName)

		// Save
		m := c.data
		assert.NoError(t, m.Save(tempDir))

		// Load file
		file, err := os.ReadFile(path)
		assert.NoError(t, err)
		assert.Equal(t, utils.EscapeWhitespaces(c.json), utils.EscapeWhitespaces(string(file)))
	}
}

func TestValidateEmpty(t *testing.T) {
	m := &Manifest{}
	err := m.Validate()
	assert.NotNil(t, err)
	expected := `Manifest is not valid:
- key="version", value="0", failed "required" validation
- key="project", value="<nil>", failed "required" validation
`
	assert.Equal(t, expected, err.Error())
}

func TestValidateMinimal(t *testing.T) {
	m := minimalStruct()
	err := m.Validate()
	assert.Nil(t, err)
}

func TestValidateFull(t *testing.T) {
	m := fullStruct()
	err := m.Validate()
	assert.Nil(t, err)
}

func TestValidateBadVersion(t *testing.T) {
	m := minimalStruct()
	m.Version = 123
	err := m.Validate()
	assert.NotNil(t, err)
	expected := `Manifest is not valid:
- key="version", value="123", failed "max" validation
`
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
		Version: 1,
		Project: &Project{
			Id:      12345,
			ApiHost: "keboola.connection.com",
		},
		Branches:       make([]*Branch, 0),
		Configurations: make([]*Configuration, 0),
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
      "id": 11,
      "componentId": "keboola.ex-db-oracle",
      "branchId": 10,
      "path": "11-raw-data",
      "rows": [
        {
          "id": 101,
          "path": "101-region-1.json"
        },
        {
          "id": 102,
          "path": "102-region-2.json"
        }
      ]
    },
    {
      "id": 12,
      "componentId": "keboola.wr-db-mysql",
      "branchId": 11,
      "path": "12-current-month",
      "rows": [
        {
          "id": 103,
          "path": "103-all.json"
        },
        {
          "id": 104,
          "path": "104-sum.json"
        }
      ]
    }
  ]
}`
}

func fullStruct() *Manifest {
	return &Manifest{
		Version: 1,
		Project: &Project{
			Id:      12345,
			ApiHost: "keboola.connection.com",
		},
		Branches: []*Branch{
			{
				Id:   10,
				Path: "main",
			},
			{
				Id:   11,
				Path: "11-dev",
			},
		},
		Configurations: []*Configuration{
			{
				Id:          11,
				ComponentId: "keboola.ex-db-oracle",
				BranchId:    10,
				Path:        "11-raw-data",
				Rows: []*Row{
					{
						Id:   101,
						Path: "101-region-1.json",
					},
					{
						Id:   102,
						Path: "102-region-2.json",
					},
				},
			},
			{
				Id:          12,
				ComponentId: "keboola.wr-db-mysql",
				BranchId:    11,
				Path:        "12-current-month",
				Rows: []*Row{
					{
						Id:   103,
						Path: "103-all.json",
					},
					{
						Id:   104,
						Path: "104-sum.json",
					},
				},
			},
		},
	}
}
