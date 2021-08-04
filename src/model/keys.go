package model

import (
	"fmt"

	"github.com/spf13/cast"
)

const (
	BranchAbbr    = "B"
	ComponentAbbr = "COM"
	ConfigAbbr    = "C"
	RowAbbr       = "R"
)

type Key interface {
	Kind() Kind
	String() string
}

type Object interface {
	Level() int // hierarchical level, "1" for branch, "2" for config, ...
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

type ConfigRowKey struct {
	BranchId    int    `json:"-" validate:"required"`
	ComponentId string `json:"-" validate:"required"`
	ConfigId    string `json:"-" validate:"required"`
	Id          string `json:"id" validate:"required" `
}

func (k BranchKey) Kind() Kind {
	return Kind{Name: "branch", Abbr: BranchAbbr}
}

func (k ComponentKey) Kind() Kind {
	return Kind{Name: "component", Abbr: ComponentAbbr}
}

func (k ConfigKey) Kind() Kind {
	return Kind{Name: "config", Abbr: ConfigAbbr}
}

func (k ConfigRowKey) Kind() Kind {
	return Kind{Name: "config row", Abbr: RowAbbr}
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

func (k BranchKey) String() string {
	return fmt.Sprintf("%02d_%d_branch", k.Level(), k.Id)
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

func (k ConfigKey) ComponentKey() *ComponentKey {
	return &ComponentKey{Id: k.ComponentId}
}

func (k ConfigKey) BranchKey() *BranchKey {
	return &BranchKey{Id: k.BranchId}
}

func (k ConfigRowKey) ComponentKey() *ComponentKey {
	return &ComponentKey{Id: k.ComponentId}
}

func (k ConfigRowKey) ConfigKey() *ConfigKey {
	return &ConfigKey{BranchId: k.BranchId, ComponentId: k.ComponentId, Id: k.ConfigId}
}
