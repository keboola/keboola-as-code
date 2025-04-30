package model

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
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
	ObjectID() string
	ParentKey() (Key, error) // unique key of the parent object
}

type WithKey interface {
	Key() Key
}

type BranchKey struct {
	ID keboola.BranchID `json:"id" validate:"required"`
}

type ConfigKey struct {
	BranchID    keboola.BranchID    `json:"branchId,omitempty" validate:"required_in_project"`
	ComponentID keboola.ComponentID `json:"componentId" validate:"required"`
	ID          keboola.ConfigID    `json:"id" validate:"required"`
}

type ConfigRowKey struct {
	BranchID    keboola.BranchID    `json:"-" validate:"required_in_project"`
	ComponentID keboola.ComponentID `json:"-" validate:"required"`
	ConfigID    keboola.ConfigID    `json:"-" validate:"required"`
	ID          keboola.RowID       `json:"id" validate:"required" `
}

type BlockKey struct {
	BranchID    keboola.BranchID    `json:"-" validate:"required_in_project" `
	ComponentID keboola.ComponentID `json:"-" validate:"required" `
	ConfigID    keboola.ConfigID    `json:"-" validate:"required" `
	Index       int                 `json:"-" validate:"min=0" `
}

type CodeKey struct {
	BranchID    keboola.BranchID    `json:"-" validate:"required_in_project" `
	ComponentID keboola.ComponentID `json:"-" validate:"required" `
	ConfigID    keboola.ConfigID    `json:"-" validate:"required" `
	BlockIndex  int                 `json:"-" validate:"min=0" `
	Index       int                 `json:"-" validate:"min=0" `
}

type PhaseKey struct {
	BranchID    keboola.BranchID    `json:"-" validate:"required_in_project" `
	ComponentID keboola.ComponentID `json:"-" validate:"required" `
	ConfigID    keboola.ConfigID    `json:"-" validate:"required" `
	Index       int                 `json:"-" validate:"min=0" `
}

type TaskKey struct {
	PhaseKey `json:"-"`
	Index    int `json:"-" validate:"min=0"`
}

func (k BranchKey) Kind() Kind {
	return Kind{Name: BranchKind, Abbr: BranchAbbr}
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

func (k BranchKey) ObjectID() string {
	return k.ID.String()
}

func (k ConfigKey) ObjectID() string {
	return k.ID.String()
}

func (k ConfigRowKey) ObjectID() string {
	return k.ID.String()
}

func (k BlockKey) ObjectID() string {
	return cast.ToString(k.Index)
}

func (k CodeKey) ObjectID() string {
	return cast.ToString(k.Index)
}

func (k PhaseKey) ObjectID() string {
	return cast.ToString(k.Index)
}

func (k TaskKey) ObjectID() string {
	return cast.ToString(k.Index)
}

func (k BranchKey) Level() int {
	return 1
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
		BranchID:    k.BranchID,
		ComponentID: k.ComponentID,
		ID:          k.ConfigID,
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
		BranchID:    k.BranchID,
		ComponentID: k.ComponentID,
		ID:          k.ConfigID,
	}
}

func (k CodeKey) BlockKey() Key {
	return BlockKey{
		BranchID:    k.BranchID,
		ComponentID: k.ComponentID,
		ConfigID:    k.ConfigID,
		Index:       k.BlockIndex,
	}
}

func (k CodeKey) ParentKey() (Key, error) {
	return k.BlockKey(), nil
}

func (k BranchKey) Desc() string {
	return fmt.Sprintf(`%s "%d"`, k.Kind().Name, k.ID)
}

func (k ConfigKey) Desc() string {
	if k.BranchID == 0 {
		// Config in a template
		return fmt.Sprintf(`%s "component:%s/config:%s"`, k.Kind().Name, k.ComponentID, k.ID)
	}
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s"`, k.Kind().Name, k.BranchID, k.ComponentID, k.ID)
}

func (k ConfigRowKey) Desc() string {
	if k.BranchID == 0 {
		// Row in a template
		return fmt.Sprintf(`%s "component:%s/config:%s"`, k.Kind().Name, k.ComponentID, k.ID)
	}
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/row:%s"`, k.Kind().Name, k.BranchID, k.ComponentID, k.ConfigID, k.ID)
}

func (k BlockKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/block:%d"`, k.Kind().Name, k.BranchID, k.ComponentID, k.ConfigID, k.Index)
}

func (k CodeKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/block:%d/code:%d"`, k.Kind().Name, k.BranchID, k.ComponentID, k.ConfigID, k.BlockIndex, k.Index)
}

func (k PhaseKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/phase:%d"`, k.Kind().Name, k.BranchID, k.ComponentID, k.ConfigID, k.Index)
}

func (k TaskKey) Desc() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/phase:%d/task:%d"`, k.Kind().Name, k.BranchID, k.ComponentID, k.ConfigID, k.PhaseKey.Index, k.Index)
}

func (k BranchKey) String() string {
	return fmt.Sprintf("%02d_%d_branch", k.Level(), k.ID)
}

func (k BranchKey) ParentKey() (Key, error) {
	return nil, nil // Branch is top level object
}

func (k ConfigKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_config", k.Level(), k.BranchID, k.ComponentID, k.ID)
}

func (k ConfigRowKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%s_config_row", k.Level(), k.BranchID, k.ComponentID, k.ConfigID, k.ID)
}

func (k BlockKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%03d_block", k.Level(), k.BranchID, k.ComponentID, k.ConfigID, k.Index)
}

func (k CodeKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%03d_%03d_code", k.Level(), k.BranchID, k.ComponentID, k.ConfigID, k.BlockIndex, k.Index)
}

func (k PhaseKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%03d_phase", k.Level(), k.BranchID, k.ComponentID, k.ConfigID, k.Index)
}

func (k TaskKey) String() string {
	return fmt.Sprintf("%02d_%d_%s_%s_%03d_%03d_task", k.Level(), k.BranchID, k.ComponentID, k.ConfigID, k.PhaseKey.Index, k.Index)
}

func (k ConfigKey) BranchKey() BranchKey {
	return BranchKey{ID: k.BranchID}
}

func (k ConfigKey) ParentKey() (Key, error) {
	if k.BranchID == 0 {
		// Configs in template are not related to any branch
		return nil, nil
	}
	return k.BranchKey(), nil
}

func (k ConfigRowKey) BranchKey() BranchKey {
	return k.ConfigKey().BranchKey()
}

func (k ConfigRowKey) ConfigKey() ConfigKey {
	return ConfigKey{BranchID: k.BranchID, ComponentID: k.ComponentID, ID: k.ConfigID}
}

func (k ConfigRowKey) ParentKey() (Key, error) {
	return k.ConfigKey(), nil
}

func (b Block) ConfigKey() ConfigKey {
	return ConfigKey{BranchID: b.BranchID, ComponentID: b.ComponentID, ID: b.ConfigID}
}

func (c Code) ConfigKey() ConfigKey {
	return ConfigKey{BranchID: c.BranchID, ComponentID: c.ComponentID, ID: c.ConfigID}
}

func (k PhaseKey) ConfigKey() ConfigKey {
	return ConfigKey{
		BranchID:    k.BranchID,
		ComponentID: k.ComponentID,
		ID:          k.ConfigID,
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

type ConfigIDMetadata struct {
	IDInTemplate keboola.ConfigID `json:"idInTemplate"`
}

type RowIDMetadata struct {
	IDInProject  keboola.RowID `json:"idInProject"`
	IDInTemplate keboola.RowID `json:"idInTemplate"`
}
