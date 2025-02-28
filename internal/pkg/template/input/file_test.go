package input

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
)

func TestLoadInputsFile(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()

	// Write file
	path := Path()
	require.NoError(t, fs.WriteFile(t.Context(), filesystem.NewRawFile(path, inputsJsonnet)))

	// Load
	inputs, err := Load(t.Context(), fs, jsonnet.NewContext())
	require.NoError(t, err)
	assert.Equal(t, testInputs(), inputs)
}

func TestSaveInputsFile(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	ctx := t.Context()

	// Save
	require.NoError(t, testInputs().Save(ctx, fs))

	// Load file
	file, err := fs.ReadFile(ctx, filesystem.NewFileDef(Path()))
	require.NoError(t, err)
	assert.Equal(t, wildcards.EscapeWhitespaces(inputsJsonnet), wildcards.EscapeWhitespaces(file.Content))
}

const inputsJsonnet = `{
  stepsGroups: [
    {
      description: "Group One",
      required: "all",
      steps: [
        {
          icon: "common:settings",
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
                  value: "a",
                  label: "A",
                },
                {
                  value: "b",
                  label: "B",
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

func testInputs() StepsGroups {
	return StepsGroups{
		{
			Description: "Group One",
			Required:    "all",
			Steps: []Step{
				{
					Icon:        "common:settings",
					Name:        "Step 1",
					Description: "Step One",
					Inputs: Inputs{
						{
							ID:          "fb.extractor.username",
							Name:        "Facebook username",
							Description: "Facebook username description",
							Type:        "string",
							Kind:        "input",
						},
						{
							ID:          "fb.extractor.password",
							Name:        "Facebook password",
							Description: "Facebook password description",
							Type:        "string",
							Kind:        "hidden",
						},
						{
							ID:          "fb.extractor.options",
							Name:        "Facebook options",
							Description: "Facebook options description",
							Type:        "string",
							Kind:        "select",
							Options: Options{
								{Value: "a", Label: "A"},
								{Value: "b", Label: "B"},
							},
						},
					},
				},
			},
		},
	}
}
