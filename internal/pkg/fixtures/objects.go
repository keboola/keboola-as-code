package fixtures

import (
	"fmt"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type MockedKey struct {
	Id string
}

type MockedRecord struct {
	MockedKey
	Relations model.Relations
}

type MockedObject struct {
	MockedKey
	Foo1      string
	Foo2      string
	Meta1     string                 `json:"myKey" metaFile:"true"`
	Meta2     string                 `metaFile:"true"`
	Config    *orderedmap.OrderedMap `configFile:"true"`
	Relations model.Relations
}

type MockedObjectState struct {
	*MockedRecord
	Local  *MockedObject
	Remote *MockedObject
}

type OwningSideRelation struct {
	OtherSide                   model.Key
	IgnoreMissingOtherSideValue bool
}

type OtherSideRelation struct {
	OwningSide                  model.Key
	IgnoreMissingOtherSideValue bool
}

func (MockedKey) Level() int {
	return 1
}

func (MockedKey) Kind() model.Kind {
	return model.Kind{Name: "kind", Abbr: "K"}
}

func (m MockedKey) Desc() string {
	return fmt.Sprintf(`mocked key "%s"`, m.Id)
}

func (m MockedKey) String() string {
	return "mocked_key_" + m.Id
}

func (m MockedKey) ObjectId() string {
	return m.Id
}

func (m MockedKey) ParentKey() (model.Key, error) {
	return nil, nil
}

func (r *MockedRecord) Key() model.Key {
	return r.MockedKey
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

func (r *MockedRecord) NewObjectState() model.ObjectState {
	return &MockedObjectState{MockedRecord: r}
}

func (o MockedObject) Key() model.Key {
	return o.MockedKey
}

func (MockedObject) ObjectName() string {
	return "object"
}

func (o *MockedObject) Clone() model.Object {
	clone := *o
	return &clone
}

func (r *MockedRecord) GetRelations() model.Relations {
	return r.Relations
}

func (r *MockedRecord) SetRelations(relations model.Relations) {
	r.Relations = relations
}

func (r *MockedRecord) AddRelation(relation model.Relation) {
	r.Relations.Add(relation)
}

func (o *MockedObject) GetRelations() model.Relations {
	return o.Relations
}

func (o *MockedObject) SetRelations(relations model.Relations) {
	o.Relations = relations
}

func (o *MockedObject) AddRelation(relation model.Relation) {
	o.Relations = append(o.Relations, relation)
}

func (o *MockedObjectState) ObjectName() string {
	return "object"
}

func (o *MockedObjectState) HasManifest() bool {
	return o.MockedRecord != nil
}

func (o *MockedObjectState) SetManifest(record model.Record) {
	o.MockedRecord = record.(*MockedRecord)
}

func (o *MockedObjectState) Manifest() model.Record {
	return o.MockedRecord
}

func (o *MockedObjectState) HasState(stateType model.StateType) bool {
	switch stateType {
	case model.StateTypeLocal:
		return o.Local != nil
	case model.StateTypeRemote:
		return o.Remote != nil
	default:
		panic(fmt.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (o *MockedObjectState) GetState(stateType model.StateType) model.Object {
	switch stateType {
	case model.StateTypeLocal:
		return o.Local
	case model.StateTypeRemote:
		return o.Remote
	default:
		panic(fmt.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (o *MockedObjectState) HasLocalState() bool {
	return o.Local != nil
}

func (o *MockedObjectState) SetLocalState(object model.Object) {
	if object == nil {
		o.Local = nil
	} else {
		o.Local = object.(*MockedObject)
	}
}

func (o *MockedObjectState) LocalState() model.Object {
	return o.Local
}

func (o *MockedObjectState) HasRemoteState() bool {
	return o.Remote != nil
}

func (o *MockedObjectState) SetRemoteState(object model.Object) {
	if object == nil {
		o.Remote = nil
	} else {
		o.Remote = object.(*MockedObject)
	}
}

func (o *MockedObjectState) RemoteState() model.Object {
	return o.Remote
}

func (o *MockedObjectState) LocalOrRemoteState() model.Object {
	switch {
	case o.HasLocalState():
		return o.LocalState()
	case o.HasRemoteState():
		return o.RemoteState()
	default:
		panic(fmt.Errorf("object Local or Remote state must be set"))
	}
}

func (o *MockedObjectState) RemoteOrLocalState() model.Object {
	switch {
	case o.HasRemoteState():
		return o.RemoteState()
	case o.HasLocalState():
		return o.LocalState()
	default:
		panic(fmt.Errorf("object Remote or Local state must be set"))
	}
}

func (r *OwningSideRelation) Type() model.RelationType {
	return "owning_side_relation"
}

func (r *OwningSideRelation) ParentKey(_ model.Key) (model.Key, error) {
	return nil, nil
}

func (r *OwningSideRelation) OtherSideKey(_ model.Key) model.Key {
	return r.OtherSide
}

func (r *OwningSideRelation) IsOwningSide() bool {
	return true
}

func (r *OwningSideRelation) IgnoreMissingOtherSide() bool {
	return r.IgnoreMissingOtherSideValue
}

func (r *OwningSideRelation) NewOtherSideRelation(owner model.Key) model.Relation {
	if r.OtherSide != nil {
		return &OtherSideRelation{
			OwningSide: owner,
		}
	}
	return nil
}

func (r *OtherSideRelation) Type() model.RelationType {
	return "other_side_relation"
}

func (r *OtherSideRelation) ParentKey(_ model.Key) (model.Key, error) {
	return nil, nil
}

func (r *OtherSideRelation) OtherSideKey(_ model.Key) model.Key {
	return r.OwningSide
}

func (r *OtherSideRelation) IsOwningSide() bool {
	return false
}

func (r *OtherSideRelation) IgnoreMissingOtherSide() bool {
	return r.IgnoreMissingOtherSideValue
}

func (r *OtherSideRelation) NewOtherSideRelation(owner model.Key) model.Relation {
	if r.OwningSide != nil {
		return &OwningSideRelation{
			OtherSide: owner,
		}
	}
	return nil
}
