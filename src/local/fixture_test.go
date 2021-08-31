package local

import (
	"github.com/iancoleman/orderedmap"

	"keboola-as-code/src/model"
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

func (m ModelStruct) ObjectId() string {
	return "123"
}

func (m ModelStruct) Kind() model.Kind {
	return m.Key().Kind()
}

func (ModelStruct) IsMarkedToDelete() bool {
	return false
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

func (MockedRecord) RelativePath() string {
	return `test`
}

func (MockedRecord) GetRelatedPaths() []string {
	return nil
}

func (MockedRecord) AddRelatedPath(path string) {
	// nop
}
