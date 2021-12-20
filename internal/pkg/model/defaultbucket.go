package model

import (
	"fmt"
)

// UsedInConfigInputMappingRelation indicates that the owner config is used in a config input mapping.
type UsedInConfigInputMappingRelation struct {
	UsedIn ConfigKey
}

func (t *UsedInConfigInputMappingRelation) Type() RelationType {
	return UsedInConfigInputMappingRelType
}

func (t *UsedInConfigInputMappingRelation) Desc() string {
	return fmt.Sprintf(`used in input mapping "%s"`, t.UsedIn.Desc())
}

func (t *UsedInConfigInputMappingRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.UsedIn.ObjectId())
}

func (t *UsedInConfigInputMappingRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *UsedInConfigInputMappingRelation) OtherSideKey(_ Key) Key {
	return nil
}

func (t *UsedInConfigInputMappingRelation) IsDefinedInManifest() bool {
	return false
}

func (t *UsedInConfigInputMappingRelation) IsDefinedInApi() bool {
	return false
}

func (t *UsedInConfigInputMappingRelation) NewOtherSideRelation(_ Object, _ Objects) (Key, Relation, error) {
	return nil, nil, nil
}

// UsedInRowInputMappingRelation indicates that the owner config is used in a row input mapping.
type UsedInRowInputMappingRelation struct {
	UsedIn ConfigRowKey
}

func (t *UsedInRowInputMappingRelation) Type() RelationType {
	return UsedInRowInputMappingRelType
}

func (t *UsedInRowInputMappingRelation) Desc() string {
	return fmt.Sprintf(`used in input mapping "%s"`, t.UsedIn.Desc())
}

func (t *UsedInRowInputMappingRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.UsedIn.ObjectId())
}

func (t *UsedInRowInputMappingRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *UsedInRowInputMappingRelation) OtherSideKey(_ Key) Key {
	return nil
}

func (t *UsedInRowInputMappingRelation) IsDefinedInManifest() bool {
	return false
}

func (t *UsedInRowInputMappingRelation) IsDefinedInApi() bool {
	return false
}

func (t *UsedInRowInputMappingRelation) NewOtherSideRelation(_ Object, _ Objects) (Key, Relation, error) {
	return nil, nil, nil
}
