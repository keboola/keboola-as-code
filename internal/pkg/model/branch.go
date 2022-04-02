package model

import (
	"fmt"
	"strconv"

	"github.com/spf13/cast"
)

var BranchKind = Kind{Name: "branch", Abbr: "B"}

type BranchId int

type BranchKey struct {
	Id BranchId `json:"id" validate:"required"`
}

// Branch https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
type Branch struct {
	BranchKey
	Name        string `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description string `json:"description" diff:"true" descriptionFile:"true"`
	IsDefault   bool   `json:"isDefault" diff:"true" metaFile:"true"`
}

func (k Kind) IsBranch() bool {
	return k == BranchKind
}

func (v BranchId) String() string {
	return strconv.Itoa(int(v))
}

func (k BranchKey) Level() ObjectLevel {
	return 10
}

func (k BranchKey) Kind() Kind {
	return BranchKind
}

func (k BranchKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k BranchKey) LogicPath() string {
	return fmt.Sprintf(`branch:%d`, k.Id)
}

func (k BranchKey) Key() Key {
	return k
}

func (k BranchKey) ParentKey() (Key, error) {
	return nil, nil // Branch is top level object
}

func (k BranchKey) ObjectId() string {
	return cast.ToString(k.Id)
}

func (k BranchKey) NewObject() Object {
	return &Branch{BranchKey: k}
}

func (k BranchKey) NewObjectManifest() ObjectManifest {
	return &BranchManifest{BranchKey: k}
}

func (b Branch) ObjectName() string {
	return b.Name
}
