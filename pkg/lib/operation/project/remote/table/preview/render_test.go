package preview

import (
	"strings"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	input    *keboola.TablePreview
	expected string
}

func TestRenderJSON(t *testing.T) {
	t.Parallel()

	cases := []testCase{
		{
			input: &keboola.TablePreview{
				Columns: []string{"Id", "Name", "Region", "Status", "First_Order"},
				Rows: [][]string{
					{"f030ed64cbc8babbe50901a26675a2ee", "CSK Auto", "US West", "Active", "2015-01-23"},
					{"06c0b954b0d2088e3da2132d1ba96f31", "AM/PM Camp", "Global", "Active", "2015-02-04"},
					{"fffe0e30b4a34f01063330a4b908fde5", "Super Saver Foods", "Global", "Active", "2015-02-06"},
					{"33025ad4a425b6ee832e76beb250ae1c", "Netcore", "Global", "Inactive", "2015-03-02"},
				},
			},
			expected: `{"columns":["Id","Name","Region","Status","First_Order"],"rows":[["f030ed64cbc8babbe50901a26675a2ee","CSK Auto","US West","Active","2015-01-23"],["06c0b954b0d2088e3da2132d1ba96f31","AM/PM Camp","Global","Active","2015-02-04"],["fffe0e30b4a34f01063330a4b908fde5","Super Saver Foods","Global","Active","2015-02-06"],["33025ad4a425b6ee832e76beb250ae1c","Netcore","Global","Inactive","2015-03-02"]]}`,
		},
	}

	for _, c := range cases {
		actual := renderJSON(c.input)
		assert.Equal(t, c.expected, actual)
	}
}

func TestRenderCSV(t *testing.T) {
	t.Parallel()

	cases := []testCase{
		{
			input: &keboola.TablePreview{
				Columns: []string{"Id", "Name", "Region", "Status", "First_Order"},
				Rows: [][]string{
					{"f030ed64cbc8babbe50901a26675a2ee", "CSK Auto", "US West", "Active", "2015-01-23"},
					{"06c0b954b0d2088e3da2132d1ba96f31", "AM/PM Camp", "Global", "Active", "2015-02-04"},
					{"fffe0e30b4a34f01063330a4b908fde5", "Super Saver Foods", "Global", "Active", "2015-02-06"},
					{"33025ad4a425b6ee832e76beb250ae1c", "Netcore", "Global", "Inactive", "2015-03-02"},
				},
			},
			expected: `Id,Name,Region,Status,First_Order
f030ed64cbc8babbe50901a26675a2ee,CSK Auto,US West,Active,2015-01-23
06c0b954b0d2088e3da2132d1ba96f31,AM/PM Camp,Global,Active,2015-02-04
fffe0e30b4a34f01063330a4b908fde5,Super Saver Foods,Global,Active,2015-02-06
33025ad4a425b6ee832e76beb250ae1c,Netcore,Global,Inactive,2015-03-02`,
		},
	}

	for _, c := range cases {
		actual := renderCSV(c.input)
		// using `expected` without normalizing, because the output should not have an empty line at the end
		assert.Equal(t, c.expected, actual)
	}
}

func TestRenderPretty(t *testing.T) {
	t.Parallel()

	cases := []testCase{
		{
			input: &keboola.TablePreview{
				Columns: []string{"Id", "Name", "Region", "Status", "First_Order"},
				Rows: [][]string{
					{"f030ed64cbc8babbe50901a26675a2ee", "CSK Auto", "US West", "Active", "2015-01-23"},
					{"06c0b954b0d2088e3da2132d1ba96f31", "AM/PM Camp", "Global", "Active", "2015-02-04"},
					{"fffe0e30b4a34f01063330a4b908fde5", "Super Saver Foods", "Global", "Active", "2015-02-06"},
					{"33025ad4a425b6ee832e76beb250ae1c", "Netcore", "Global", "Inactive", "2015-03-02"},
				},
			},
			expected: `
				┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┓
				┃ Id                               ┃ Name              ┃ Region  ┃ Status   ┃ First_Order ┃
				┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━━━━━━━╋━━━━━━━━━╋━━━━━━━━━━╋━━━━━━━━━━━━━┫
				┃ f030ed64cbc8babbe50901a26675a2ee ┃ CSK Auto          ┃ US West ┃ Active   ┃ 2015-01-23  ┃
				┃ 06c0b954b0d2088e3da2132d1ba96f31 ┃ AM/PM Camp        ┃ Global  ┃ Active   ┃ 2015-02-04  ┃
				┃ fffe0e30b4a34f01063330a4b908fde5 ┃ Super Saver Foods ┃ Global  ┃ Active   ┃ 2015-02-06  ┃
				┃ 33025ad4a425b6ee832e76beb250ae1c ┃ Netcore           ┃ Global  ┃ Inactive ┃ 2015-03-02  ┃
				┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━┻━━━━━━━━━━┻━━━━━━━━━━━━━┛
			`,
		},
	}

	for _, c := range cases {
		actual := normalizeTable(renderPretty(c.input, false))
		expected := normalizeTable(c.expected)

		assert.Equal(t, expected, actual)
	}
}

func normalizeTable(s string) string {
	s = strings.TrimSpace(s)
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	return strings.Join(lines, "\n")
}
