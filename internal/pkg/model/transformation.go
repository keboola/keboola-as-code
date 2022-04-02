package model

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/sql"
)

var (
	TransformationKind = Kind{Name: "transformation", Abbr: "t", ToMany: false}
	BlockKind          = Kind{Name: "block", Abbr: "b", ToMany: true}
	CodeKind           = Kind{Name: "code", Abbr: "c", ToMany: true}
)

type TransformationKey struct {
	ConfigKey `validate:"dive" `
}

type BlockKey struct {
	TransformationKey `validate:"dive" `
	BlockIndex        int `validate:"min=0" `
}

type CodeKey struct {
	BlockKey  `json:"-" validate:"dive" `
	CodeIndex int `json:"-" validate:"min=0" `
}

type UsedSharedCodeRows []ConfigRowKey

type LinkToSharedCode struct {
	Config ConfigKey `validate:"dive" `
}

type Transformation struct {
	TransformationKey `validate:"dive" `
	LinkToSharedCode  *LinkToSharedCode `validate:"dive" diff:"true"`
}

// Block - transformation block.
type Block struct {
	BlockKey `validate:"dive" `
	Name     string `json:"name" validate:"required" metaFile:"true"  diff:"true"`
}

type Codes []*Code

// Code - transformation code.
type Code struct {
	CodeKey `validate:"dive" `
	Name    string  `json:"name" validate:"required" metaFile:"true"  diff:"true"`
	Scripts Scripts `json:"script" validate:"dive"  diff:"true"` // scripts, eg. SQL statements
}

type Scripts []Script

// StaticScript is script defined by user (it is not link to shared code).
type StaticScript struct {
	Value string
}

func (k Kind) IsTransformation() bool {
	return k == TransformationKind
}

func (k Kind) IsBlock() bool {
	return k == BlockKind
}

func (k Kind) IsCode() bool {
	return k == CodeKind
}

func (k TransformationKey) Kind() Kind {
	return TransformationKind
}

func (k TransformationKey) Level() ObjectLevel {
	return 45
}

func (k TransformationKey) Key() Key {
	return k
}

func (k TransformationKey) ParentKey() (Key, error) {
	return k.ConfigKey, nil
}

func (k TransformationKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k TransformationKey) LogicPath() string {
	return k.ConfigKey.LogicPath() + "/transformation"
}

func (k TransformationKey) ObjectId() string {
	return "transformation"
}

func (k BlockKey) Kind() Kind {
	return BlockKind
}

func (k BlockKey) Level() ObjectLevel {
	return 46
}

func (k BlockKey) Key() Key {
	return k
}

func (k BlockKey) ParentKey() (Key, error) {
	return k.TransformationKey, nil
}

func (k BlockKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k BlockKey) LogicPath() string {
	return k.TransformationKey.LogicPath() + fmt.Sprintf("/block:%03d", k.BlockIndex+1)
}

func (k BlockKey) ObjectId() string {
	return cast.ToString(k.BlockIndex)
}

func (k CodeKey) Kind() Kind {
	return CodeKind
}

func (k CodeKey) Level() ObjectLevel {
	return 47
}

func (k CodeKey) Key() Key {
	return k
}

func (k CodeKey) ParentKey() (Key, error) {
	return k.BlockKey, nil
}

func (k CodeKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k CodeKey) LogicPath() string {
	return k.BlockKey.LogicPath() + fmt.Sprintf("/code:%03d", k.CodeIndex+1)
}

func (k CodeKey) ObjectId() string {
	return cast.ToString(k.CodeIndex)
}

type Script interface {
	Content() string
}

func (v UsedSharedCodeRows) IdsSlice() []interface{} {
	var ids []interface{}
	for _, rowKey := range v {
		ids = append(ids, rowKey.ConfigRowId.String())
	}
	return ids
}

//func (v *Transformation) VisitCodes(callback func(code *Code)) {
//	for _, block := range v.Blocks {
//		for _, code := range block.Codes {
//			callback(code)
//		}
//	}
//}
//
//func (v *Transformation) VisitScripts(callback func(code *Code, script Script)) {
//	for _, block := range v.Blocks {
//		for _, code := range block.Codes {
//			for _, script := range code.Scripts {
//				callback(code, script)
//			}
//		}
//	}
//}
//
//func (v *Transformation) MapScripts(callback func(block *Block, code *Code, script Script) Script) {
//	for _, block := range v.Blocks {
//		for _, code := range block.Codes {
//			for index, script := range code.Scripts {
//				code.Scripts[index] = callback(block, code, script)
//			}
//		}
//	}
//}

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
