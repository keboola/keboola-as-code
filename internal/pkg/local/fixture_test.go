package local

import (
	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type MockedKey struct{}

type MockedRecord struct {
	MockedKey
}

type MockedObject struct {
	MockedRecord
	Foo1   string
	Foo2   string
	Meta1  string                 `json:"myKey" metaFile:"true"`
	Meta2  string                 `metaFile:"true"`
	Config *orderedmap.OrderedMap `configFile:"true"`
}

func (MockedKey) Level() int {
	return 1
}

func (MockedKey) Kind() model.Kind {
	return model.Kind{Name: "kind", Abbr: "K"}
}

func (MockedKey) Desc() string {
	return "key"
}

func (MockedKey) String() string {
	return "key"
}

func (m MockedKey) ObjectId() string {
	return "123"
}

func (m MockedKey) ParentKey() (model.Key, error) {
	return nil, nil
}

func (MockedRecord) Key() model.Key {
	return &MockedKey{}
}

func (MockedRecord) ParentKey() (model.Key, error) {
	return nil, nil
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

func (MockedRecord) IsParentPathSet() bool {
	return true
}

func (MockedRecord) SetParentPath(string) {
}

func (MockedRecord) Path() string {
	return `test`
}

func (MockedRecord) GetRelatedPaths() []string {
	return nil
}

func (MockedRecord) AddRelatedPath(path string) {
	// nop
}

func (r MockedRecord) NewEmptyObject() model.Object {
	return &MockedObject{}
}

func (MockedObject) ObjectName() string {
	return "object"
}

func (o *MockedObject) Clone() model.Object {
	clone := *o
	return &clone
}
