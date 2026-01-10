package blockcode

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// BlocksToString converts a slice of blocks to a single string with comment markers.
// This matches the UI implementation in helpers.js getBlocksAsString().
func BlocksToString(blocks []*model.Block, componentID keboola.ComponentID, sharedCodes map[string]string) string {
	d := GetDelimiter(componentID)

	var result strings.Builder

	// Filter blocks: only include blocks with non-empty codes
	filteredBlocks := filterNonEmptyBlocks(blocks, sharedCodes)

	for i, block := range filteredBlocks {
		if i > 0 {
			// Add blank line between blocks
			result.WriteString("\n")
		}

		// Write block marker
		result.WriteString(formatBlockMarker(block.Name, d))
		result.WriteString("\n")

		// Write codes
		for j, code := range block.Codes {
			// Add blank line before code marker
			result.WriteString("\n")

			// Determine if this is a shared code reference
			isShared := isSharedCode(code, sharedCodes)

			// Write code marker
			result.WriteString(formatCodeMarker(code.Name, isShared, d))
			result.WriteString("\n")

			// Write script content
			script := getScriptContent(code, sharedCodes)
			script = prepareScript(script, d)
			result.WriteString(script)

			// Add newline after script for separation from next code/block
			// But not after the very last code of the last block
			isLastCodeOfLastBlock := (i == len(filteredBlocks)-1) && (j == len(block.Codes)-1)
			if !isLastCodeOfLastBlock {
				result.WriteString("\n")
			}
		}
	}

	return result.String()
}

// formatBlockMarker creates a block marker string.
func formatBlockMarker(name string, d Delimiter) string {
	return fmt.Sprintf("%s===== BLOCK: %s =====%s", d.Start, name, d.End)
}

// formatCodeMarker creates a code marker string.
func formatCodeMarker(name string, isShared bool, d Delimiter) string {
	if isShared {
		return fmt.Sprintf("%s===== SHARED CODE: %s =====%s", d.Start, name, d.End)
	}
	return fmt.Sprintf("%s===== CODE: %s =====%s", d.Start, name, d.End)
}

// filterNonEmptyBlocks returns only blocks that have codes with actual content.
func filterNonEmptyBlocks(blocks []*model.Block, sharedCodes map[string]string) []*model.Block {
	var result []*model.Block
	for _, block := range blocks {
		hasContent := false
		for _, code := range block.Codes {
			if len(code.Scripts) > 0 && code.Scripts[0].Content() != "" {
				hasContent = true
				break
			}
			if isSharedCode(code, sharedCodes) {
				hasContent = true
				break
			}
		}
		if hasContent {
			result = append(result, block)
		}
	}
	return result
}

// isSharedCode checks if a code references shared code.
func isSharedCode(code *model.Code, sharedCodes map[string]string) bool {
	if len(code.Scripts) == 0 {
		return false
	}
	content := code.Scripts[0].Content()
	// Check if content is a shared code placeholder like {{sharedCodeId}}
	if strings.HasPrefix(content, "{{") && strings.HasSuffix(content, "}}") {
		return true
	}
	return false
}

// getScriptContent returns the script content, expanding shared code references.
func getScriptContent(code *model.Code, sharedCodes map[string]string) string {
	if len(code.Scripts) == 0 {
		return ""
	}

	content := code.Scripts[0].Content()

	// If it's a shared code reference, try to expand it
	if strings.HasPrefix(content, "{{") && strings.HasSuffix(content, "}}") {
		id := strings.TrimPrefix(strings.TrimSuffix(content, "}}"), "{{")
		if expanded, ok := sharedCodes[id]; ok {
			return expanded
		}
	}

	// Join all scripts for SQL (multiple statements)
	if len(code.Scripts) > 1 {
		var parts []string
		for _, script := range code.Scripts {
			parts = append(parts, script.Content())
		}
		return strings.Join(parts, "\n")
	}

	return content
}

// prepareScript prepares a script for output by ensuring proper delimiter handling.
func prepareScript(script string, d Delimiter) string {
	script = strings.TrimSpace(script)
	if script == "" {
		return ""
	}

	// Check if script already ends with proper delimiter or comment
	if d.Stmt != "" && !strings.HasSuffix(script, d.Stmt) {
		// Check if it ends with a comment
		if !endsWithComment(script, d) {
			script += d.Stmt
		}
	}

	return script
}

// endsWithComment checks if a script ends with a comment.
func endsWithComment(script string, d Delimiter) bool {
	lines := strings.Split(script, "\n")
	if len(lines) == 0 {
		return false
	}
	lastLine := strings.TrimSpace(lines[len(lines)-1])

	// Check for comment end (SQL block comments)
	if d.End != "" && strings.HasSuffix(lastLine, strings.TrimSpace(d.End)) {
		return true
	}

	// Check for inline comments
	for _, inline := range d.Inline {
		if strings.Contains(lastLine, inline) {
			return true
		}
	}

	return false
}
