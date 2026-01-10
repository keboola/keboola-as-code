package blockcode

import (
	"regexp"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// ParsedBlock represents a parsed block from a single-file transformation.
type ParsedBlock struct {
	Name  string
	Codes []ParsedCode
}

// ParsedCode represents a parsed code from a single-file transformation.
type ParsedCode struct {
	Name     string
	IsShared bool
	Script   string
}

// ParseString parses a single-file transformation into blocks and codes.
// This matches the UI implementation in helpers.js prepareMultipleBlocks().
func ParseString(content string, componentID keboola.ComponentID) []ParsedBlock {
	d := GetDelimiter(componentID)

	// Build regex patterns matching the UI implementation
	// Block regex: {commentStart}\s?=+\s?block:\s?([^=]+)=+\s?{commentEnd}
	blockPattern := buildBlockRegex(d)
	// Code regex: {commentStart}\s?=+\s?(code|shared code):\s?([^=]+)=+\s?{commentEnd}
	codePattern := buildCodeRegex(d)

	// Split by block markers
	blockParts := blockPattern.Split(content, -1)
	blockNames := blockPattern.FindAllStringSubmatch(content, -1)

	// If no blocks found, create a default block with the entire content
	if len(blockNames) == 0 {
		return []ParsedBlock{
			{
				Name: "New Code Block",
				Codes: []ParsedCode{
					{
						Name:   "New Code",
						Script: strings.TrimSpace(content),
					},
				},
			},
		}
	}

	var blocks []ParsedBlock

	// Process each block
	for i, match := range blockNames {
		blockName := strings.TrimSpace(match[1])
		blockContent := ""
		if i+1 < len(blockParts) {
			blockContent = blockParts[i+1]
		}

		// Parse codes within this block
		codes := parseCodesFromBlock(blockContent, codePattern)

		// If no codes found in block, create a default code with the block content
		if len(codes) == 0 && strings.TrimSpace(blockContent) != "" {
			codes = []ParsedCode{
				{
					Name:   "New Code",
					Script: strings.TrimSpace(blockContent),
				},
			}
		}

		blocks = append(blocks, ParsedBlock{
			Name:  blockName,
			Codes: codes,
		})
	}

	return blocks
}

// parseCodesFromBlock parses codes from a block's content.
func parseCodesFromBlock(content string, codePattern *regexp.Regexp) []ParsedCode {
	// Split by code markers
	codeParts := codePattern.Split(content, -1)
	codeMatches := codePattern.FindAllStringSubmatch(content, -1)

	if len(codeMatches) == 0 {
		return nil
	}

	var codes []ParsedCode

	for i, match := range codeMatches {
		codeType := strings.ToLower(strings.TrimSpace(match[1]))
		codeName := strings.TrimSpace(match[2])
		codeContent := ""
		if i+1 < len(codeParts) {
			codeContent = strings.TrimSpace(codeParts[i+1])
		}

		codes = append(codes, ParsedCode{
			Name:     codeName,
			IsShared: codeType == "shared code",
			Script:   codeContent,
		})
	}

	return codes
}

// buildBlockRegex builds the regex pattern for matching block markers.
func buildBlockRegex(d Delimiter) *regexp.Regexp {
	// Escape special regex characters in delimiters
	start := regexp.QuoteMeta(strings.TrimSpace(d.Start))
	end := regexp.QuoteMeta(strings.TrimSpace(d.End))

	// Pattern: {commentStart}\s?=+\s?block:\s?([^=]+)=+\s?{commentEnd}
	pattern := start + `\s*=+\s*[Bb][Ll][Oo][Cc][Kk]:\s*([^=]+?)=+\s*` + end

	return regexp.MustCompile(pattern)
}

// buildCodeRegex builds the regex pattern for matching code markers.
func buildCodeRegex(d Delimiter) *regexp.Regexp {
	// Escape special regex characters in delimiters
	start := regexp.QuoteMeta(strings.TrimSpace(d.Start))
	end := regexp.QuoteMeta(strings.TrimSpace(d.End))

	// Pattern: {commentStart}\s?=+\s?(code|shared code):\s?([^=]+)=+\s?{commentEnd}
	pattern := start + `\s*=+\s*([Cc][Oo][Dd][Ee]|[Ss][Hh][Aa][Rr][Ee][Dd]\s+[Cc][Oo][Dd][Ee]):\s*([^=]+?)=+\s*` + end

	return regexp.MustCompile(pattern)
}

// ToBlocks converts parsed blocks to model.Block objects.
func ToBlocks(parsed []ParsedBlock, config *model.Config, componentID keboola.ComponentID) []*model.Block {
	blocks := make([]*model.Block, 0, len(parsed))

	for blockIndex, pb := range parsed {
		block := &model.Block{
			BlockKey: model.BlockKey{
				BranchID:    config.BranchID,
				ComponentID: config.ComponentID,
				ConfigID:    config.ID,
				Index:       blockIndex,
			},
			Name:  pb.Name,
			Codes: make([]*model.Code, 0, len(pb.Codes)),
		}

		for codeIndex, pc := range pb.Codes {
			code := &model.Code{
				CodeKey: model.CodeKey{
					BranchID:    config.BranchID,
					ComponentID: config.ComponentID,
					ConfigID:    config.ID,
					BlockIndex:  blockIndex,
					Index:       codeIndex,
				},
				Name:    pc.Name,
				Scripts: model.ScriptsFromStr(pc.Script, componentID),
			}
			block.Codes = append(block.Codes, code)
		}

		blocks = append(blocks, block)
	}

	return blocks
}
