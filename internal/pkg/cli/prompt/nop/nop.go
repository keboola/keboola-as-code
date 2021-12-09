package nop

import (
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
)

type Prompt struct{}

func New() prompt.Prompt {
	return &Prompt{}
}

func (p *Prompt) IsInteractive() bool {
	return false
}

func (p *Prompt) Printf(_ string, _ ...interface{}) {
	// nop
}

func (p *Prompt) Confirm(c *prompt.Confirm) bool {
	return c.Default
}

func (p *Prompt) Ask(q *prompt.Question) (result string, ok bool) {
	return q.Default, true
}

func (p *Prompt) Select(s *prompt.Select) (value string, ok bool) {
	return s.Default, s.UseDefault
}

func (p *Prompt) SelectIndex(s *prompt.SelectIndex) (index int, ok bool) {
	return s.Default, s.UseDefault
}

func (p *Prompt) MultiSelect(s *prompt.MultiSelect) (result []string, ok bool) {
	return s.Default, true
}

func (p *Prompt) MultiSelectIndex(s *prompt.MultiSelectIndex) (result []int, ok bool) {
	return s.Default, true
}

func (p *Prompt) Multiline(q *prompt.Question) (result string, ok bool) {
	return q.Default, true
}

func (p *Prompt) Editor(q *prompt.Question) (result string, ok bool) {
	return q.Default, true
}
