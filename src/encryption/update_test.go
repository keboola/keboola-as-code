package encryption

import (
	"keboola-as-code/src/utils"
	"testing"

	"github.com/iancoleman/orderedmap"
	"github.com/stretchr/testify/assert"
)

func TestUpdateMapStep(t *testing.T) {
	content := utils.PairsToOrderedMap([]utils.Pair{
		{
			Key:   "key1",
			Value: "value1",
		},
		{
			Key:   "key2",
			Value: "value1",
		},
		{
			Key: "parameters",
			Value: *utils.PairsToOrderedMap([]utils.Pair{
				{
					Key:   "host",
					Value: "mysql.example.com",
				},
			}),
		},
	})
	keyPath := path{mapStep("parameters"), mapStep("host")}
	content = UpdateContent(content, keyPath, "newValue")
	parameters, _ := content.Get("parameters")
	p := parameters.(orderedmap.OrderedMap)
	host, _ := p.Get("host")
	assert.Equal(t, host, "newValue")
}

func TestUpdateSliceStep(t *testing.T) {
	content := utils.PairsToOrderedMap([]utils.Pair{
		{
			Key:   "key1",
			Value: "value1",
		},
		{
			Key:   "key2",
			Value: "value1",
		},
		{
			Key: "parameters",
			Value: *utils.PairsToOrderedMap([]utils.Pair{
				{
					Key:   "host",
					Value: "mysql.example.com",
				},
				{
					Key: "values",
					Value: []interface{}{
						*utils.PairsToOrderedMap([]utils.Pair{
							{
								Key:   "name",
								Value: "john",
							},
						}),
						*utils.PairsToOrderedMap([]utils.Pair{
							{
								Key:   "name",
								Value: "kate",
							},
						}),
					},
				},
			}),
		},
	})
	keyPath := path{mapStep("parameters"), mapStep("values"), sliceStep(1), mapStep("name")}
	content = UpdateContent(content, keyPath, "newValue")
	parameters, _ := content.Get("parameters")
	parametersMap := parameters.(orderedmap.OrderedMap)
	values, _ := parametersMap.Get("values")
	secondName := values.([]interface{})[1]
	secondNameMap := secondName.(orderedmap.OrderedMap)
	name, _ := secondNameMap.Get("name")
	assert.Equal(t, name, "newValue")
}
