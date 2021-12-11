package model

import (
	"fmt"
)

// UsedInInputMappingRelation indicates that the owner config is used in an input mapping.
type UsedInInputMappingRelation struct {
	ConfigKey Key
}

func (t *UsedInInputMappingRelation) Type() RelationType {
	return UsedInInputMappingRelType
}

func (t *UsedInInputMappingRelation) Desc() string {
	return fmt.Sprintf(`used in input mapping "%s"`, t.ConfigKey.Desc())
}

func (t *UsedInInputMappingRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.ConfigKey.ObjectId())
}

func (t *UsedInInputMappingRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *UsedInInputMappingRelation) OtherSideKey(_ Key) Key {
	return nil
}

func (t *UsedInInputMappingRelation) IsDefinedInManifest() bool {
	return false
}

func (t *UsedInInputMappingRelation) IsDefinedInApi() bool {
	return false
}

func (t *UsedInInputMappingRelation) NewOtherSideRelation(_ Object, _ *StateObjects) (Key, Relation, error) {
	return nil, nil, nil
}
