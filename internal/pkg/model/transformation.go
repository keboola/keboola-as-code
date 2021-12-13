package model

import (
	"bytes"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
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
	PathInProject `json:"-"`
	Name          string `json:"name" validate:"required" metaFile:"true"`
	Codes         Codes  `json:"codes" validate:"omitempty,dive"`
}

type Codes []*Code

// Code - transformation code.
type Code struct {
	CodeKey
	PathInProject `json:"-"`
	CodeFileName  string   `json:"-"` // eg. "code.sql", "code.py", ...
	Name          string   `json:"name" validate:"required" metaFile:"true"`
	Scripts       []string `json:"script"` // scripts, eg. SQL statements
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

func (v *Transformation) VisitScripts(callback func(code *Code, script string)) {
	for _, block := range v.Blocks {
		for _, code := range block.Codes {
			for _, script := range code.Scripts {
				callback(code, script)
			}
		}
	}
}

func (v *Transformation) MapScripts(callback func(code *Code, script string) string) {
	v.VisitCodes(func(code *Code) {
		for index, script := range code.Scripts {
			code.Scripts[index] = callback(code, script)
		}
	})
}

func (v *Transformation) Clone() *Transformation {
	if v == nil {
		return nil
	}
	clone := *v
	clone.LinkToSharedCode = v.LinkToSharedCode.Clone()
	clone.Blocks = make([]*Block, len(v.Blocks))
	for i, block := range v.Blocks {
		clone.Blocks[i] = block.Clone()
	}
	return &clone
}

func (v *LinkToSharedCode) Clone() *LinkToSharedCode {
	if v == nil {
		return nil
	}
	clone := *v
	clone.Rows = make([]ConfigRowKey, len(v.Rows))
	for i, row := range v.Rows {
		clone.Rows[i] = row
	}
	return &clone
}

func (k Kind) IsBlock() bool {
	return k.Name == BlockKind
}

func (k Kind) IsCode() bool {
	return k.Name == CodeKind
}

func (b *Block) Clone() *Block {
	clone := *b
	clone.Codes = b.Codes.Clone()
	return &clone
}

func (c *Code) Clone() *Code {
	clone := *c
	clone.Scripts = make([]string, len(c.Scripts))
	for index, script := range c.Scripts {
		clone.Scripts[index] = script
	}
	return &clone
}

func (v Codes) Clone() Codes {
	if v == nil {
		return nil
	}

	out := make(Codes, len(v))
	for index, item := range v {
		out[index] = item.Clone()
	}
	return out
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
	return fmt.Sprintf("## %s\n%s", c.Name, c.ScriptsToString())
}

func (c Code) ScriptsToString() string {
	return strhelper.TransformationScriptsToString(c.Scripts, c.ComponentId.String())
}
