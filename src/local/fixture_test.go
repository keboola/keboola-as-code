package local

import (
	"keboola-as-code/src/model"

	"github.com/iancoleman/orderedmap"
)

type ModelStruct struct {
	Foo1   string
	Foo2   string
	Meta1  string                 `json:"myKey" metaFile:"true"`
	Meta2  string                 `metaFile:"true"`
	Config *orderedmap.OrderedMap `configFile:"true"`
}

type MockedKey struct{}
type MockedRecord struct{}

func (MockedKey) Kind() model.Kind {
	return model.Kind{Name: "kind", Abbr: "K"}
}

func (MockedKey) String() string {
	return "key"
}

func (ModelStruct) Key() model.Key {
	return &MockedKey{}
}

func (ModelStruct) Level() int {
	return 1
}

func (MockedRecord) Key() model.Key {
	return &MockedKey{}
}

func (MockedRecord) Level() int {
	return 1
}

func (r MockedRecord) Kind() model.Kind {
	return r.Key().Kind()
}

func (MockedRecord) State() *model.RecordState {
	return &model.RecordState{}
}

func (MockedRecord) SortKey(sort string) string {
	return "key"
}

func (m MockedRecord) RelativePath() string {
	return `test`
}

func (m MockedRecord) GetRelatedPaths() []string {
	return nil
}

func (m MockedRecord) AddRelatedPath(path string) {
	// nop
}
