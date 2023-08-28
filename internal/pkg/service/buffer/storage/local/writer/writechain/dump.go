package writechain

import (
	"fmt"
	"strings"
)

func (c *Chain) Dump() string {
	var out strings.Builder

	out.WriteString("Writers:\n")
	for _, item := range c.writers {
		out.WriteString("  ")
		out.WriteString(stringOrType(item))
		out.WriteString("\n")
	}

	out.WriteString("\nFlushers:\n")
	for _, item := range c.flushers {
		out.WriteString("  ")
		out.WriteString(stringOrType(item))
		out.WriteString("\n")
	}

	out.WriteString("\nClosers:\n")
	for _, item := range c.closers {
		out.WriteString("  ")
		out.WriteString(stringOrType(item))
		out.WriteString("\n")
	}

	return out.String()
}

func stringOrType(v any) string {
	if stringer, ok := v.(fmt.Stringer); ok {
		return stringer.String()
	} else {
		return fmt.Sprintf("%T", v)
	}
}
