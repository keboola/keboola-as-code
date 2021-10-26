package model

import (
	"fmt"

	"github.com/spf13/cast"
)

const (
	BranchKind    = "branch"
	ComponentKind = "component"
	ConfigKind    = "config"
	ConfigRowKind = "config row"
	BlockKind     = "block"
	CodeKind      = "code"
	BranchAbbr    = "B"
	ComponentAbbr = "COM"
	ConfigAbbr    = "C"
	RowAbbr       = "R"
	BlockAbbr     = "b"
	CodeAbbr      = "c"
)

type Key interface {
	Level() int     // hierarchical level, "1" for branch, "2" for config, ...
	Kind() Kind     // kind of the object: branch, config, ...
	Desc() string   // human-readable description of the object
	String() string // unique string representation of the key
	ObjectId() string
	ParentKey() (Key, error) // unique key of the parent object
}

type WithKey interface {
	Key() Key
}

type BranchKey struct {
	Id int `json:"id" validate:"required,min=1"`
}

type ComponentKey struct {
	Id string `json:"id" validate:"required"`
}

type ConfigKey struct {
	BranchId    int    `json:"branchId" validate:"required"`
	ComponentId string `json:"componentId" validate:"required"`
	Id          string `json:"id" validate:"required"`
}

type ConfigKeySameBranch struct {
	ComponentId string `json:"componentId" validate:"required"`
	Id          string `json:"id" validate:"required"`
}

type ConfigRowKey struct {
	BranchId    int    `json:"-" validate:"required"`
	ComponentId string `json:"-" validate:"required"`
	ConfigId    string `json:"-" validate:"required"`
	Id          string `json:"id" validate:"required" `
}

type BlockKey struct {
	BranchId    int    `json:"-"`
	ComponentId string `json:"-"`
	ConfigId    string `json:"-"`
	Index       int    `json:"-"`
}

type CodeKey struct {
	BranchId    int    `json:"-"`
	ComponentId string `json:"-"`
	ConfigId    string `json:"-"`
	BlockIndex  int    `json:"-"`
	Index       int    `json:"-"`
}

func (k BranchKey) Kind() Kind {
	return Kind{Name: BranchKind, Abbr: BranchAbbr}
}

func (k ComponentKey) Kind() Kind {
	return Kind{Name: ComponentKind, Abbr: ComponentAbbr}
}

func (k ConfigKey) Kind() Kind {
	return Kind{Name: ConfigKind, Abbr: ConfigAbbr}
}

func (k ConfigRowKey) Kind() Kind {
	return Kind{Name: ConfigRowKind, Abbr: RowAbbr}
}

func (k BlockKey) Kind() Kind {
	return Kind{Name: BlockKind, Abbr: BlockAbbr}
}

func (k CodeKey) Kind() Kind {
	return Kind{Name: CodeKind, Abbr: CodeAbbr}
}

func (k BranchKey) ObjectId() string {
	return cast.ToString(k.Id)
}

func (k ComponentKey) ObjectId() string {
	return k.Id
}

func (k ConfigKey) ObjectId() string {
	return k.Id
}

func (k ConfigRowKey) ObjectId() string {
	return k.Id
}

func (k BlockKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k CodeKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k BranchKey) Level() int {
	return 1
}

func (k ComponentKey) Level() int {
	return 2
}

func (k ConfigKey) Level() int {
	return 3
}

func (k ConfigRowKey) Level() int {
	return 4
}

func (k BlockKey) Level() int {
	return 5
}

func (k CodeKey) Level() int {
	return 6
}

func (k BranchKey) Key() Key {
	return k
}

func (k ComponentKey) Key() Key {
	return k
}

func (k ConfigKey) Key() Key {
	return k
}

func (k ConfigRowKey) Key() Key {
	return k
}

func (k BlockKey) Key() Key {
	return k
}

func (k BlockKey) ConfigKey() Key {
	return ConfigKey{
		BranchId:    k.BranchId,
		ComponentId: k.ComponentId,
		Id:          k.ConfigId,
	}
}

func (k BlockKey) ParentKey() (Key, error) {
	return k.ConfigKey(), nil
}

func (k CodeKey) Key() Key {
	return k
}

func (k CodeKey) ConfigKey() Key {
	return ConfigKey{
		BranchId:    k.BranchId,
		ComponentId: k.ComponentId,
		Id:          k.ConfigId,
	}
}

func (k CodeKey) BlockKey() Key {
	return BlockKey{
		BranchId:    k.BranchId,
		ComponentId: k.ComponentId,
		ConfigId:    k.ConfigId,
		Index:       k.BlockIndex,
	}
}

func (k CodeKey) ParentKey() (Key, error) {
	return k.BlockKey(), nil
}

func (k ComponentKey) ParentKey() (Key, error) {
	return nil, nil // Component is top level object
}

func (k BranchKey) Desc() string {
	return fmt.Sprintf(`%s "%d"`, k.Kind().Name, k.Id)
}

func (k ComponentKey) Desc() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.Id)
}

func (k ConfigKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s"`, k.Kind().Name, k.BranchId, k.ComponentId, k.Id)
}

func (k ConfigRowKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/row:%s"`, k.Kind().Name, k.BranchId, k.ComponentId, k.ConfigId, k.Id)
}

func (k BlockKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/block:%d"`, k.Kind().Name, k.BranchId, k.ComponentId, k.ConfigId, k.Index)
}

func (k CodeKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/block:%d/code:%d"`, k.Kind().Name, k.BranchId, k.ComponentId, k.ConfigId, k.BlockIndex, k.Index)
}

func (k BranchKey) String() string {
	return fmt.Sprintf("%02d_%d_branch", k.Level(), k.Id)
}

func (k BranchKey) ParentKey() (Key, error) {
	return nil, nil // Branch is top level object
}

func (k ComponentKey) String() string {
	return fmt.Sprintf("%02d_%s_component", k.Level(), k.Id)
}

func (k ConfigKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_config", k.Level(), k.BranchId, k.ComponentId, k.Id)
}

func (k ConfigRowKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%s_config_row", k.Level(), k.BranchId, k.ComponentId, k.ConfigId, k.Id)
}

func (k BlockKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%03d_block", k.Level(), k.BranchId, k.ComponentId, k.ConfigId, k.Index)
}

func (k CodeKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%03d_%03d_code", k.Level(), k.BranchId, k.ComponentId, k.ConfigId, k.BlockIndex, k.Index)
}

func (k ConfigKey) ComponentKey() *ComponentKey {
	return &ComponentKey{Id: k.ComponentId}
}

func (k ConfigKey) BranchKey() *BranchKey {
	return &BranchKey{Id: k.BranchId}
}

func (k ConfigKey) ParentKey() (Key, error) {
	return k.BranchKey(), nil
}

func (k ConfigRowKey) ComponentKey() *ComponentKey {
	return &ComponentKey{Id: k.ComponentId}
}

func (k ConfigRowKey) BranchKey() *BranchKey {
	return k.ConfigKey().BranchKey()
}

func (k ConfigRowKey) ConfigKey() *ConfigKey {
	return &ConfigKey{BranchId: k.BranchId, ComponentId: k.ComponentId, Id: k.ConfigId}
}

func (k ConfigRowKey) ParentKey() (Key, error) {
	return k.ConfigKey(), nil
}

func (b Block) ConfigKey() *ConfigKey {
	return &ConfigKey{BranchId: b.BranchId, ComponentId: b.ComponentId, Id: b.ConfigId}
}

func (c Code) ConfigKey() *ConfigKey {
	return &ConfigKey{BranchId: c.BranchId, ComponentId: c.ComponentId, Id: c.ConfigId}
}

func (k ConfigKeySameBranch) ConfigKey(branch BranchKey) *ConfigKey {
	return &ConfigKey{BranchId: branch.Id, ComponentId: k.ComponentId, Id: k.Id}
}
