package fixtures

import (
	"fmt"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type MockedKey struct {
	ID string
}

type MockedManifest struct {
	MockedKey
	*model.RecordState
	PathValue    string
	Relations    model.Relations
	RelatedPaths []string
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
	*MockedManifest
	Local  *MockedObject
	Remote *MockedObject
	Ignore bool
}

type MockedManifestSideRelation struct {
	OtherSide model.Key
}

type MockedAPISideRelation struct {
	OtherSide model.Key
}

func (MockedKey) Level() int {
	return 1
}

func (MockedKey) Kind() model.Kind {
	return model.Kind{Name: "kind", Abbr: "K"}
}

func (m MockedKey) Desc() string {
	return fmt.Sprintf(`mocked key "%s"`, m.ID)
}

func (m MockedKey) String() string {
	return "mocked_key_" + m.ID
}

func (m MockedKey) ObjectID() string {
	return m.ID
}

func (m *MockedKey) SetObjectID(id any) {
	m.ID = id.(string)
}

func (m MockedKey) ParentKey() (model.Key, error) {
	return nil, nil
}

func (r *MockedManifest) Key() model.Key {
	return r.MockedKey
}

func (MockedManifest) ParentKey() (model.Key, error) {
	return nil, nil
}

func (r MockedManifest) Kind() model.Kind {
	return r.Key().Kind()
}

func (r *MockedManifest) State() *model.RecordState {
	if r.RecordState == nil {
		return &model.RecordState{}
	}
	return r.RecordState
}

func (MockedManifest) SortKey(_ string) string {
	return "key"
}

func (MockedManifest) GetRelativePath() string {
	return "foo"
}

func (MockedManifest) SetRelativePath(string) {
}

func (MockedManifest) GetParentPath() string {
	return "bar"
}

func (MockedManifest) IsParentPathSet() bool {
	return true
}

func (MockedManifest) SetParentPath(string) {
}

func (r MockedManifest) GetAbsPath() model.AbsPath {
	if len(r.PathValue) > 0 {
		return model.NewAbsPath("", r.PathValue)
	}
	return model.NewAbsPath("", "test")
}

func (r MockedManifest) Path() string {
	if len(r.PathValue) > 0 {
		return r.PathValue
	}
	return `test`
}

func (r *MockedManifest) ClearRelatedPaths() {
	r.RelatedPaths = make([]string, 0)
}

func (r *MockedManifest) GetRelatedPaths() []string {
	return r.RelatedPaths
}

func (r *MockedManifest) AddRelatedPath(path string) {
	r.RelatedPaths = append(r.RelatedPaths, path)
}

func (r *MockedManifest) AddRelatedPathInRoot(path string) {
	r.AddRelatedPath(path)
}

func (MockedManifest) RenameRelatedPaths(_, _ string) {
	// nop
}

func (r MockedManifest) NewEmptyObject() model.Object {
	return &MockedObject{}
}

func (r *MockedManifest) NewObjectState() model.ObjectState {
	return &MockedObjectState{MockedManifest: r}
}

func (o MockedObject) Key() model.Key {
	return o.MockedKey
}

func (MockedObject) ObjectName() string {
	return "object"
}

func (r *MockedManifest) GetRelations() model.Relations {
	return r.Relations
}

func (r *MockedManifest) SetRelations(relations model.Relations) {
	r.Relations = relations
}

func (r *MockedManifest) AddRelation(relation model.Relation) {
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
	return o.MockedManifest != nil
}

func (o *MockedObjectState) IsIgnored() bool {
	return o.Ignore
}

func (o *MockedObjectState) SetManifest(manifest model.ObjectManifest) {
	o.MockedManifest = manifest.(*MockedManifest)
}

func (o *MockedObjectState) Manifest() model.ObjectManifest {
	return o.MockedManifest
}

func (o *MockedObjectState) HasState(stateType model.StateType) bool {
	switch stateType {
	case model.StateTypeLocal:
		return o.Local != nil
	case model.StateTypeRemote:
		return o.Remote != nil
	default:
		panic(errors.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (o *MockedObjectState) GetState(stateType model.StateType) model.Object {
	switch stateType {
	case model.StateTypeLocal:
		return o.Local
	case model.StateTypeRemote:
		return o.Remote
	default:
		panic(errors.Errorf(`unexpected state type "%T"`, stateType))
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
		panic(errors.New("object Local or Remote state must be set"))
	}
}

func (o *MockedObjectState) RemoteOrLocalState() model.Object {
	switch {
	case o.HasRemoteState():
		return o.RemoteState()
	case o.HasLocalState():
		return o.LocalState()
	default:
		panic(errors.New("object Remote or Local state must be set"))
	}
}

func (r *MockedManifestSideRelation) Type() model.RelationType {
	return "manifest_side_relation"
}

func (r *MockedManifestSideRelation) Desc() string {
	return "manifest side relation"
}

func (r *MockedManifestSideRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, r.Type(), r.OtherSide.String())
}

func (r *MockedManifestSideRelation) ParentKey(_ model.Key) (model.Key, error) {
	return nil, nil
}

func (r *MockedManifestSideRelation) IsDefinedInManifest() bool {
	return true
}

func (r *MockedManifestSideRelation) IsDefinedInAPI() bool {
	return false
}

func (r *MockedManifestSideRelation) NewOtherSideRelation(relationDefinedOn model.Object, _ model.Objects) (model.Key, model.Relation, error) {
	if r.OtherSide != nil {
		return r.OtherSide, &MockedAPISideRelation{OtherSide: relationDefinedOn.Key()}, nil
	}
	return nil, nil, nil
}

func (r *MockedAPISideRelation) Type() model.RelationType {
	return "api_side_relation"
}

func (r *MockedAPISideRelation) Desc() string {
	return "api side relation"
}

func (r *MockedAPISideRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, r.Type(), r.OtherSide.String())
}

func (r *MockedAPISideRelation) ParentKey(_ model.Key) (model.Key, error) {
	return nil, nil
}

func (r *MockedAPISideRelation) OtherSideKey(_ model.Key) model.Key {
	return r.OtherSide
}

func (r *MockedAPISideRelation) IsDefinedInManifest() bool {
	return false
}

func (r *MockedAPISideRelation) IsDefinedInAPI() bool {
	return true
}

func (r *MockedAPISideRelation) NewOtherSideRelation(relationDefinedOn model.Object, _ model.Objects) (model.Key, model.Relation, error) {
	if r.OtherSide != nil {
		return r.OtherSide, &MockedManifestSideRelation{OtherSide: relationDefinedOn.Key()}, nil
	}
	return nil, nil, nil
}
