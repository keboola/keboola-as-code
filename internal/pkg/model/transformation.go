package model

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/sql"
)

const (
	TransformationKind = "transformation"
	BlockKind          = "block"
	CodeKind           = "code"
	TransformationAbbr = "t"
	BlockAbbr          = "b"
	CodeAbbr           = "c"
)

type TransformationKey struct {
	ConfigKey
}

type BlockKey struct {
	Parent ConfigKey `json:"-" validate:"dive" `
	Index  int       `json:"-" validate:"min=0" `
}

type CodeKey struct {
	Parent BlockKey `json:"-" validate:"dive" `
	Index  int      `json:"-" validate:"min=0" `
}

type UsedSharedCodeRows []ConfigRowKey

type LinkToSharedCode struct {
	Config ConfigKey
	Rows   UsedSharedCodeRows
}

type Transformation struct {
	TransformationKey
	Blocks           []*Block
	LinkToSharedCode *LinkToSharedCode
}

// Block - transformation block.
type Block struct {
	BlockKey
	Name  string `json:"name" validate:"required" metaFile:"true"`
	Codes Codes  `json:"codes" validate:"omitempty,dive"`
}

type Codes []*Code

// Code - transformation code.
type Code struct {
	CodeKey
	Name    string  `json:"name" validate:"required" metaFile:"true"`
	Scripts Scripts `json:"script"` // scripts, eg. SQL statements
}

type Scripts []Script

// StaticScript is script defined by user (it is not link to shared code).
type StaticScript struct {
	Value string
}

func (k TransformationKey) Kind() Kind {
	return Kind{Name: TransformationKind, Abbr: TransformationAbbr}
}

func (k TransformationKey) Level() ObjectLevel {
	return 3
}

func (k TransformationKey) Key() Key {
	return k
}

func (k TransformationKey) ParentKey() (Key, error) {
	return k.ConfigKey, nil
}

func (k BlockKey) Kind() Kind {
	return Kind{Name: BlockKind, Abbr: BlockAbbr}
}

func (k BlockKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k BlockKey) Level() ObjectLevel {
	return 4
}

func (k BlockKey) Key() Key {
	return k
}

func (k BlockKey) ParentKey() (Key, error) {
	return k.Parent, nil
}

func (k BlockKey) String() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/block:%d"`, k.Kind().Name, k.Parent.BranchId, k.Parent.ComponentId, k.Parent.Id, k.Index)
}

func (b Block) ObjectName() string {
	return b.ObjectId()
}

func (k CodeKey) Kind() Kind {
	return Kind{Name: CodeKind, Abbr: CodeAbbr}
}

func (k CodeKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k CodeKey) Level() ObjectLevel {
	return 5
}

func (k CodeKey) Key() Key {
	return k
}

func (k CodeKey) ParentKey() (Key, error) {
	return k.Parent, nil
}

func (k CodeKey) ComponentId() ComponentId {
	return k.Parent.Parent.ComponentId
}

func (k CodeKey) String() string {
	ck := k.Parent.Parent
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/block:%d/code:%d"`, k.Kind().Name, ck.BranchId, ck.ComponentId, ck.Id, k.Parent.Index, k.Index)
}

func (c Code) ObjectName() string {
	return c.ObjectId()
}

type Script interface {
	Content() string
}

func (v UsedSharedCodeRows) IdsSlice() []interface{} {
	var ids []interface{}
	for _, rowKey := range v {
		ids = append(ids, rowKey.Id.String())
	}
	return ids
}

func (v *Transformation) VisitCodes(callback func(code *Code)) {
	for _, block := range v.Blocks {
		for _, code := range block.Codes {
			callback(code)
		}
	}
}

func (v *Transformation) VisitScripts(callback func(code *Code, script Script)) {
	for _, block := range v.Blocks {
		for _, code := range block.Codes {
			for _, script := range code.Scripts {
				callback(code, script)
			}
		}
	}
}

func (v *Transformation) MapScripts(callback func(block *Block, code *Code, script Script) Script) {
	for _, block := range v.Blocks {
		for _, code := range block.Codes {
			for index, script := range code.Scripts {
				code.Scripts[index] = callback(block, code, script)
			}
		}
	}
}

func (k Kind) IsBlock() bool {
	return k.Name == BlockKind
}

func (k Kind) IsCode() bool {
	return k.Name == CodeKind
}

func (v Scripts) Slice() []interface{} {
	var out []interface{}
	for _, script := range v {
		out = append(out, script.Content())
	}
	return out
}

func (v Scripts) String(componentId ComponentId) string {
	var items []string
	for _, script := range v {
		items = append(items, script.Content())
	}

	switch componentId.String() {
	case `keboola.snowflake-transformation`:
		fallthrough
	case `keboola.synapse-transformation`:
		fallthrough
	case `keboola.oracle-transformation`:
		return sql.Join(items) + "\n"
	default:
		return strings.Join(items, "\n") + "\n"
	}
}

// MarshalJSON converts Scripts to JSON []string.
func (v Scripts) MarshalJSON() ([]byte, error) {
	var scripts []string
	for _, script := range v {
		scripts = append(scripts, script.(StaticScript).Value)
	}
	return json.Marshal(scripts)
}

// UnmarshalJSON converts JSON string or []string to Scripts.
func (v *Scripts) UnmarshalJSON(data []byte) error {
	var script string
	var scripts []string
	if err := json.Unmarshal(data, &script); err == nil {
		scripts = append(scripts, script)
	} else if err := json.Unmarshal(data, &scripts); err != nil {
		return err
	}

	*v = make(Scripts, 0)
	for _, item := range scripts {
		*v = append(*v, StaticScript{Value: item})
	}
	return nil
}

func (v StaticScript) Content() string {
	return v.Value
}

func NormalizeScript(script string) string {
	return strings.TrimRight(script, "\n\r\t ")
}

func ScriptsFromStr(content string, componentId ComponentId) Scripts {
	content = NormalizeScript(content)
	var items []string
	switch componentId.String() {
	case `keboola.snowflake-transformation`:
		fallthrough
	case `keboola.synapse-transformation`:
		fallthrough
	case `keboola.oracle-transformation`:
		items = sql.Split(content)
	default:
		items = []string{content}
	}

	scripts := make(Scripts, 0)
	for _, item := range items {
		scripts = append(scripts, StaticScript{Value: item})
	}
	return scripts
}

func ScriptsFromSlice(items []interface{}) Scripts {
	var scripts Scripts
	for _, item := range items {
		scripts = append(scripts, StaticScript{Value: cast.ToString(item)})
	}
	return scripts
}
