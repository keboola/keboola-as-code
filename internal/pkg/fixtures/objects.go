package fixtures

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type MockedKey struct {
	Id string
}

type MockedManifest struct {
	MockedKey
	PathValue    model.AbsPath
	Relations    model.Relations
	RelatedPaths []string
}

type MockedObject struct {
	MockedKey
	Foo1      string
	Foo2      string
	Meta1     string                 `json:"myKey" metaFile:"true"`
	Meta2     string                 `metaFile:"true"`
	Config    *orderedmap.OrderedMap `configFile:"true" diff:"true"`
	Relations model.Relations        `diff:"true"`
}

type MockedManifestSideRelation struct {
	OtherSide model.Key
}

type MockedApiSideRelation struct {
	OtherSide model.Key
}

func (MockedKey) Level() model.ObjectLevel {
	return 1
}

func (MockedKey) Kind() model.Kind {
	return model.Kind{Name: "kind", Abbr: "K"}
}

func (m MockedKey) Desc() string {
	return fmt.Sprintf(`mocked key "%s"`, m.Id)
}

func (m MockedKey) String() string {
	return fmt.Sprintf(`mocked key "%s"`, m.Id)
}

func (m MockedKey) ObjectId() string {
	return m.Id
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

func (r MockedManifest) Path() model.AbsPath {
	if r.PathValue.IsSet() {
		return r.PathValue
	}
	return model.NewAbsPath("", "test")
}

func (r MockedManifest) SetPath(v model.AbsPath) {
	r.PathValue = v
}

func (r MockedManifest) String() string {
	return r.Key().String()
}

func (r MockedManifest) NewEmptyObject() model.Object {
	return &MockedObject{}
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

func (r *MockedManifestSideRelation) Type() model.RelationType {
	return "manifest_side_relation"
}

func (r *MockedManifestSideRelation) String() string {
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

func (r *MockedManifestSideRelation) IsDefinedInApi() bool {
	return false
}

func (r *MockedManifestSideRelation) NewOtherSideRelation(relationDefinedOn model.Object, _ model.Objects) (model.Key, model.Relation, error) {
	if r.OtherSide != nil {
		return r.OtherSide, &MockedApiSideRelation{OtherSide: relationDefinedOn.Key()}, nil
	}
	return nil, nil, nil
}

func (r *MockedApiSideRelation) Type() model.RelationType {
	return "api_side_relation"
}

func (r *MockedApiSideRelation) String() string {
	return "api side relation"
}

func (r *MockedApiSideRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, r.Type(), r.OtherSide.String())
}

func (r *MockedApiSideRelation) ParentKey(_ model.Key) (model.Key, error) {
	return nil, nil
}

func (r *MockedApiSideRelation) OtherSideKey(_ model.Key) model.Key {
	return r.OtherSide
}

func (r *MockedApiSideRelation) IsDefinedInManifest() bool {
	return false
}

func (r *MockedApiSideRelation) IsDefinedInApi() bool {
	return true
}

func (r *MockedApiSideRelation) NewOtherSideRelation(relationDefinedOn model.Object, _ model.Objects) (model.Key, model.Relation, error) {
	if r.OtherSide != nil {
		return r.OtherSide, &MockedManifestSideRelation{OtherSide: relationDefinedOn.Key()}, nil
	}
	return nil, nil, nil
}
