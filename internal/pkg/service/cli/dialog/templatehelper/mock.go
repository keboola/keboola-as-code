package templatehelper

import (
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func AddMockedObjectsResponses(httpTransport *httpmock.MockTransport) {
	configJSON := `
{
  "storage": {
    "foo": "bar"
  },
  "parameters": {
    "string": "my string",
    "#password": "my password",
    "int": 123
  }
}
`
	configContent := orderedmap.New()
	json.MustDecodeString(configJSON, configContent)

	branches := []*model.Branch{{BranchKey: model.BranchKey{ID: 123}, Name: "Main", IsDefault: true}}
	configs := []*keboola.ConfigWithRows{
		{
			Config: &keboola.Config{
				ConfigKey: keboola.ConfigKey{ID: "1"},
				Name:      `Config 1`,
				Content:   configContent,
			},
			Rows: []*keboola.ConfigRow{
				{
					ConfigRowKey: keboola.ConfigRowKey{ID: "456"},
					Name:         `My Row`,
					Content:      orderedmap.New(),
				},
			},
		},
		{Config: &keboola.Config{ConfigKey: keboola.ConfigKey{ID: "2"}, Name: `Config 2`, Content: orderedmap.New()}},
		{Config: &keboola.Config{ConfigKey: keboola.ConfigKey{ID: "3"}, Name: `Config 3`, Content: orderedmap.New()}},
	}
	components := []*keboola.ComponentWithConfigs{
		{
			Component: keboola.Component{ComponentKey: keboola.ComponentKey{ID: `keboola.my-component`}, Name: `Foo Bar`},
			Configs:   configs,
		},
	}
	httpTransport.RegisterResponder(
		"GET", `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, branches),
	)
	httpTransport.RegisterResponder(
		"GET", `=~/storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, components),
	)
}
