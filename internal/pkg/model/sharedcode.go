package model

import (
	"fmt"
)

var SharedCodeKind = Kind{Name: "shared code", Abbr: "sc", ToMany: false}

type SharedCodeKey struct {
	ConfigRowKey
}

type SharedCodeRow struct {
	SharedCodeKey
	Target  ComponentId `validate:"required"`
	Scripts Scripts     `validate:"required"`
}

// LinkScript is reference to shared code used in transformation.
type LinkScript struct {
	Target ConfigRowKey
}

func (k SharedCodeKey) Kind() Kind {
	return SharedCodeKind
}

func (k SharedCodeKey) Level() ObjectLevel {
	return 40
}

func (k SharedCodeKey) Key() Key {
	return k
}

func (k SharedCodeKey) ParentKey() (Key, error) {
	return k.ConfigRowKey, nil
}

func (k SharedCodeKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k SharedCodeKey) LogicPath() string {
	return k.ConfigRowKey.LogicPath() + "/sharedCode"
}

func (k SharedCodeKey) ObjectId() string {
	return "sharedCode"
}

func (v LinkScript) Content() string {
	return fmt.Sprintf(`shared code "%s"`, v.Target.ConfigRowId.String())
}
