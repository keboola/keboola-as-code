package model

import (
	"fmt"
	"strconv"

	"github.com/spf13/cast"
)

const (
	BranchKind    = "branch"
	ComponentKind = "component"
	ConfigKind    = "config"
	ConfigRowKind = "config row"
	BlockKind     = "block"
	CodeKind      = "code"
	PhaseKind     = "phase"
	TaskKind      = "task"
	BranchAbbr    = "B"
	ComponentAbbr = "COM"
	ConfigAbbr    = "C"
	RowAbbr       = "R"
	BlockAbbr     = "b"
	CodeAbbr      = "c"
	PhaseAbbr     = "p"
	TaskAbbr      = "t"
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

type (
	BranchId    int
	ComponentId string
	ConfigId    string
	RowId       string
)

func (v BranchId) String() string {
	return strconv.Itoa(int(v))
}

func (v ComponentId) String() string {
	return string(v)
}

func (v ConfigId) String() string {
	return string(v)
}

func (v RowId) String() string {
	return string(v)
}

type BranchKey struct {
	Id BranchId `json:"id" validate:"required"`
}

type ComponentKey struct {
	Id ComponentId `json:"id" validate:"required"`
}

type ConfigKey struct {
	BranchId    BranchId    `json:"branchId,omitempty" validate:"required_in_project"`
	ComponentId ComponentId `json:"componentId" validate:"required"`
	Id          ConfigId    `json:"id" validate:"required"`
}

type ConfigRowKey struct {
	BranchId    BranchId    `json:"-" validate:"required_in_project"`
	ComponentId ComponentId `json:"-" validate:"required"`
	ConfigId    ConfigId    `json:"-" validate:"required"`
	Id          RowId       `json:"id" validate:"required" `
}

type BlockKey struct {
	BranchId    BranchId    `json:"-" validate:"required_in_project" `
	ComponentId ComponentId `json:"-" validate:"required" `
	ConfigId    ConfigId    `json:"-" validate:"required" `
	Index       int         `json:"-" validate:"min=0" `
}

type CodeKey struct {
	BranchId    BranchId    `json:"-" validate:"required_in_project" `
	ComponentId ComponentId `json:"-" validate:"required" `
	ConfigId    ConfigId    `json:"-" validate:"required" `
	BlockIndex  int         `json:"-" validate:"min=0" `
	Index       int         `json:"-" validate:"min=0" `
}

type PhaseKey struct {
	BranchId    BranchId    `json:"-" validate:"required_in_project" `
	ComponentId ComponentId `json:"-" validate:"required" `
	ConfigId    ConfigId    `json:"-" validate:"required" `
	Index       int         `json:"-" validate:"min=0" `
}

type TaskKey struct {
	PhaseKey `json:"-" validate:"dive" `
	Index    int `json:"-" validate:"min=0" `
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

func (k PhaseKey) Kind() Kind {
	return Kind{Name: PhaseKind, Abbr: PhaseAbbr}
}

func (k TaskKey) Kind() Kind {
	return Kind{Name: TaskKind, Abbr: TaskAbbr}
}

func (k BranchKey) ObjectId() string {
	return k.Id.String()
}

func (k ComponentKey) ObjectId() string {
	return k.Id.String()
}

func (k ConfigKey) ObjectId() string {
	return k.Id.String()
}

func (k ConfigRowKey) ObjectId() string {
	return k.Id.String()
}

func (k BlockKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k CodeKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k PhaseKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k TaskKey) ObjectId() string {
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

func (k PhaseKey) Level() int {
	return 5
}

func (k TaskKey) Level() int {
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

func (k PhaseKey) Key() Key {
	return k
}

func (k TaskKey) Key() Key {
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

func (k PhaseKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/phase:%d"`, k.Kind().Name, k.BranchId, k.ComponentId, k.ConfigId, k.Index)
}

func (k TaskKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/phase:%d/task:%d"`, k.Kind().Name, k.BranchId, k.ComponentId, k.ConfigId, k.PhaseKey.Index, k.Index)
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

func (k PhaseKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%03d_phase", k.Level(), k.BranchId, k.ComponentId, k.ConfigId, k.Index)
}

func (k TaskKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%03d_%03d_task", k.Level(), k.BranchId, k.ComponentId, k.ConfigId, k.PhaseKey.Index, k.Index)
}

func (k ConfigKey) ComponentKey() ComponentKey {
	return ComponentKey{Id: k.ComponentId}
}

func (k ConfigKey) BranchKey() BranchKey {
	return BranchKey{Id: k.BranchId}
}

func (k ConfigKey) ParentKey() (Key, error) {
	if k.BranchId == 0 {
		// Configs in template are not related to any branch
		return nil, nil
	}
	return k.BranchKey(), nil
}

func (k ConfigRowKey) ComponentKey() ComponentKey {
	return ComponentKey{Id: k.ComponentId}
}

func (k ConfigRowKey) BranchKey() BranchKey {
	return k.ConfigKey().BranchKey()
}

func (k ConfigRowKey) ConfigKey() ConfigKey {
	return ConfigKey{BranchId: k.BranchId, ComponentId: k.ComponentId, Id: k.ConfigId}
}

func (k ConfigRowKey) ParentKey() (Key, error) {
	return k.ConfigKey(), nil
}

func (b Block) ConfigKey() ConfigKey {
	return ConfigKey{BranchId: b.BranchId, ComponentId: b.ComponentId, Id: b.ConfigId}
}

func (c Code) ConfigKey() ConfigKey {
	return ConfigKey{BranchId: c.BranchId, ComponentId: c.ComponentId, Id: c.ConfigId}
}

func (k PhaseKey) ConfigKey() ConfigKey {
	return ConfigKey{
		BranchId:    k.BranchId,
		ComponentId: k.ComponentId,
		Id:          k.ConfigId,
	}
}

func (k PhaseKey) ParentKey() (Key, error) {
	return k.ConfigKey(), nil
}

func (k TaskKey) ConfigKey() ConfigKey {
	return k.PhaseKey.ConfigKey()
}

func (k TaskKey) ParentKey() (Key, error) {
	return k.PhaseKey, nil
}
