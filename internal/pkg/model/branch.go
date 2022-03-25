package model

import (
	"fmt"
	"strconv"

	"github.com/spf13/cast"
)

const (
	BranchKind = "branch"
	BranchAbbr = "B"
)

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
	return k.Name == BranchKind
}

func (v BranchId) String() string {
	return strconv.Itoa(int(v))
}

func (k BranchKey) Level() int {
	return 1
}

func (k BranchKey) Kind() Kind {
	return Kind{Name: BranchKind, Abbr: BranchAbbr}
}

func (k BranchKey) String() string {
	return fmt.Sprintf(`%s "%d"`, k.Kind().Name, k.Id)
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
