package input

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestLoadInputsFile(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Write file
	path := Path()
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(path, inputsJsonNet)))

	// Load
	inputs, err := Load(fs)
	assert.NoError(t, err)
	assert.Equal(t, testInputs(), inputs)
}

func TestSaveInputsFile(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Save
	assert.NoError(t, testInputs().Save(fs))

	// Load file
	file, err := fs.ReadFile(filesystem.NewFileDef(Path()))
	assert.NoError(t, err)
	assert.Equal(t, testhelper.EscapeWhitespaces(inputsJsonNet), testhelper.EscapeWhitespaces(file.Content))
}

const inputsJsonNet = `{
  stepsGroups: [
    {
      description: "Group One",
      required: "all",
      steps: [
        {
          id: "step1",
          icon: "common",
          name: "Step 1",
          description: "Step One",
          inputs: [
            {
              id: "fb.extractor.username",
              name: "Facebook username",
              description: "Facebook username description",
              type: "string",
              kind: "input",
            },
            {
              id: "fb.extractor.password",
              name: "Facebook password",
              description: "Facebook password description",
              type: "string",
              kind: "hidden",
            },
            {
              id: "fb.extractor.options",
              name: "Facebook options",
              description: "Facebook options description",
              type: "string",
              kind: "select",
              options: [
                {
                  id: "a",
                  name: "A",
                },
                {
                  id: "b",
                  name: "B",
                },
              ],
            },
          ],
        },
      ],
    },
  ],
}
`

func testInputs() *StepsGroups {
	return &StepsGroups{
		{
			Description: "Group One",
			Required:    "all",
			Steps: []*Step{
				{
					Id:          "step1",
					Icon:        "common",
					Name:        "Step 1",
					Description: "Step One",
					Inputs: Inputs{
						{
							Id:          "fb.extractor.username",
							Name:        "Facebook username",
							Description: "Facebook username description",
							Type:        "string",
							Kind:        "input",
						},
						{
							Id:          "fb.extractor.password",
							Name:        "Facebook password",
							Description: "Facebook password description",
							Type:        "string",
							Kind:        "hidden",
						},
						{
							Id:          "fb.extractor.options",
							Name:        "Facebook options",
							Description: "Facebook options description",
							Type:        "string",
							Kind:        "select",
							Options: Options{
								{Id: "a", Name: "A"},
								{Id: "b", Name: "B"},
							},
						},
					},
				},
			},
		},
	}
}
