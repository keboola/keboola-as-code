package model

import (
	"bytes"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
)

type Transformation struct {
	Blocks []*Block
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

func (v *Transformation) Clone() *Transformation {
	if v == nil {
		return nil
	}
	clone := *v
	clone.Blocks = make([]*Block, len(v.Blocks))
	for i, block := range v.Blocks {
		clone.Blocks[i] = block.Clone()
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
