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

func (MockedKey) Desc() string {
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

func (MockedRecord) Key() model.Key {
	return &MockedKey{}
}

func (r MockedRecord) Desc() string {
	return r.Key().Desc()
}

func (MockedRecord) ParentKey() model.Key {
	return nil
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

func (MockedRecord) GetObjectPath() string {
	return "foo"
}

func (MockedRecord) SetObjectPath(string) {
}

func (MockedRecord) GetParentPath() string {
	return "bar"
}

func (MockedRecord) SetParentPath(string) {
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
