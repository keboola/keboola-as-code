package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/sql"
)

type UsedSharedCodeRows []ConfigRowKey

type LinkToSharedCode struct {
	Config ConfigKey
	Rows   UsedSharedCodeRows
}

type Transformation struct {
	Blocks           []*Block
	LinkToSharedCode *LinkToSharedCode
}

// Block - transformation block.
type Block struct {
	BlockKey
	AbsPath `json:"-"`
	Name    string `json:"name" validate:"required" metaFile:"true"`
	Codes   Codes  `json:"codes" validate:"omitempty,dive"`
}

type Codes []*Code

// Code - transformation code.
type Code struct {
	CodeKey
	AbsPath      `json:"-"`
	CodeFileName string  `json:"-"` // eg. "code.sql", "code.py", ...
	Name         string  `json:"name" validate:"required" metaFile:"true"`
	Scripts      Scripts `json:"script"` // scripts, eg. SQL statements
}

type Scripts []Script

type Script interface {
	Content() string
}

// StaticScript is script defined by user (it is not link to shared code).
type StaticScript struct {
	Value string
}

func (v UsedSharedCodeRows) IdsSlice() []any {
	ids := make([]any, 0, len(v))
	for _, rowKey := range v {
		ids = append(ids, rowKey.ID.String())
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

func (v *Transformation) MapScripts(callback func(code *Code, script Script) Script) {
	for _, block := range v.Blocks {
		for _, code := range block.Codes {
			for index, script := range code.Scripts {
				code.Scripts[index] = callback(code, script)
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

func (b Block) String() string {
	buf := new(bytes.Buffer)
	_, _ = fmt.Fprintln(buf, `# `, b.Name)
	for _, code := range b.Codes {
		_, _ = fmt.Fprint(buf, code.String())
	}
	return buf.String()
}

func (c Code) String() string {
	return fmt.Sprintf("## %s\n%s", c.Name, c.Scripts.String(c.ComponentID))
}

func (v Scripts) Slice() []any {
	out := make([]any, 0, len(v))
	for _, script := range v {
		out = append(out, script.Content())
	}
	return out
}

func (v Scripts) String(componentID keboola.ComponentID) string {
	items := make([]string, 0, len(v))
	for _, script := range v {
		items = append(items, strings.TrimRight(script.Content(), " \r\n\t"))
	}

	switch componentID.String() {
	case `keboola.snowflake-transformation`, `keboola.synapse-transformation`, `keboola.oracle-transformation`:
		return sql.Join(items) + "\n"
	default:
		return strings.Join(items, "\n") + "\n"
	}
}

// MarshalJSON converts Scripts to JSON []string.
func (v Scripts) MarshalJSON() ([]byte, error) {
	scripts := make([]string, 0, len(v))
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

	*v = make(Scripts, 0, len(scripts))
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

func ScriptsFromStr(content string, componentID keboola.ComponentID) Scripts {
	content = NormalizeScript(content)
	var items []string
	switch componentID.String() {
	case `keboola.snowflake-transformation`, `keboola.synapse-transformation`, `keboola.oracle-transformation`:
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

func ScriptsFromSlice(items []any) Scripts {
	scripts := make(Scripts, 0, len(items))
	for _, item := range items {
		scripts = append(scripts, StaticScript{Value: cast.ToString(item)})
	}
	return scripts
}
