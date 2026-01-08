package configparser

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// ParseTransformationConfig parses a Config into TransformationConfig.
func ParseTransformationConfig(componentID string, cfg *keboola.ConfigWithRows, debug bool, logger log.Logger, ctx context.Context) *TransformationConfig {
	config := &TransformationConfig{
		ID:          cfg.ID.String(),
		Name:        cfg.Name,
		ComponentID: componentID,
		Description: cfg.Description,
		IsDisabled:  cfg.IsDisabled,
		Version:     cfg.Version,
		Created:     cfg.Created.String(),
	}

	if cfg.Content == nil {
		return config
	}

	// Debug: log available keys
	if debug {
		logger.Debugf(ctx, "Config %s keys: %v", cfg.Name, cfg.Content.Keys())
	}

	// Parse storage.input.tables and storage.output.tables
	parseStorageSection(config, cfg, debug, logger, ctx)

	// Parse parameters.blocks for transformation code
	parseParametersSection(config, cfg, debug, logger, ctx)

	return config
}

// parseStorageSection parses the storage section of a transformation config.
func parseStorageSection(config *TransformationConfig, cfg *keboola.ConfigWithRows, debug bool, logger log.Logger, ctx context.Context) {
	storage, ok := cfg.Content.Get("storage")
	if !ok {
		return
	}

	storageMap := toStringMap(storage)
	if storageMap == nil {
		if debug {
			logger.Debugf(ctx, "Config %s storage type: %T", cfg.Name, storage)
		}
		return
	}

	if debug {
		logger.Debugf(ctx, "Config %s storage keys: %v", cfg.Name, maps.Keys(storageMap))
	}
	config.InputTables = ParseStorageMappings(storageMap, "input")
	config.OutputTables = ParseStorageMappings(storageMap, "output")
}

// parseParametersSection parses the parameters section of a transformation config.
func parseParametersSection(config *TransformationConfig, cfg *keboola.ConfigWithRows, debug bool, logger log.Logger, ctx context.Context) {
	params, ok := cfg.Content.Get("parameters")
	if !ok {
		return
	}

	paramsMap := toStringMap(params)
	if paramsMap == nil {
		if debug {
			logger.Debugf(ctx, "Config %s parameters type: %T", cfg.Name, params)
		}
		return
	}

	if debug {
		logger.Debugf(ctx, "Config %s parameters keys: %v", cfg.Name, maps.Keys(paramsMap))
	}
	config.Blocks = ParseCodeBlocks(paramsMap, debug, logger, ctx)
}

// toStringMap converts various map types to map[string]any.
// Handles both map[string]any and *orderedmap.OrderedMap from the SDK.
func toStringMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	// Handle orderedmap.OrderedMap (used by Keboola SDK)
	if om, ok := v.(interface {
		Keys() []string
		Get(key string) (any, bool)
	}); ok {
		result := make(map[string]any)
		for _, key := range om.Keys() {
			if val, ok := om.Get(key); ok {
				result[key] = val
			}
		}
		return result
	}
	return nil
}

// ParseStorageMappings parses input or output table mappings from storage config.
func ParseStorageMappings(storage map[string]any, key string) []StorageMapping {
	mappings := make([]StorageMapping, 0)

	section, ok := storage[key]
	if !ok {
		return mappings
	}

	sectionMap := toStringMap(section)
	if sectionMap == nil {
		return mappings
	}

	tables, ok := sectionMap["tables"]
	if !ok {
		return mappings
	}

	tablesSlice, ok := tables.([]any)
	if !ok {
		return mappings
	}

	for _, t := range tablesSlice {
		if mapping, ok := parseTableMapping(t); ok {
			mappings = append(mappings, mapping)
		}
	}

	return mappings
}

// parseTableMapping parses a single table mapping from the configuration.
func parseTableMapping(t any) (StorageMapping, bool) {
	tableMap := toStringMap(t)
	if tableMap == nil {
		return StorageMapping{}, false
	}

	mapping := StorageMapping{}
	if src, ok := tableMap["source"].(string); ok {
		mapping.Source = src
	}
	if dst, ok := tableMap["destination"].(string); ok {
		mapping.Destination = dst
	}

	if mapping.Source == "" && mapping.Destination == "" {
		return StorageMapping{}, false
	}

	return mapping, true
}

// ParseCodeBlocks parses code blocks from transformation parameters.
func ParseCodeBlocks(params map[string]any, debug bool, logger log.Logger, ctx context.Context) []*CodeBlock {
	blocks := make([]*CodeBlock, 0)

	// Parse standard blocks format
	blocks = append(blocks, parseBlocks(params, debug, logger, ctx)...)

	// Handle Snowflake/SQL transformations that use "queries" instead of "blocks"
	if queryBlock := parseQueries(params); queryBlock != nil {
		blocks = append(blocks, queryBlock)
	}

	return blocks
}

// parseBlocks parses the "blocks" section from transformation parameters.
func parseBlocks(params map[string]any, debug bool, logger log.Logger, ctx context.Context) []*CodeBlock {
	blocks := make([]*CodeBlock, 0)

	blocksRaw, ok := params["blocks"]
	if !ok {
		return blocks
	}

	blocksSlice, ok := blocksRaw.([]any)
	if !ok {
		return blocks
	}

	for _, b := range blocksSlice {
		if block := parseBlock(b, debug, logger, ctx); block != nil {
			blocks = append(blocks, block)
		}
	}

	return blocks
}

// parseBlock parses a single block from the configuration.
func parseBlock(b any, debug bool, logger log.Logger, ctx context.Context) *CodeBlock {
	blockMap := toStringMap(b)
	if blockMap == nil {
		return nil
	}

	block := &CodeBlock{}
	if name, ok := blockMap["name"].(string); ok {
		block.Name = name
	}

	if debug {
		logger.Debugf(ctx, "Block %s keys: %v", block.Name, maps.Keys(blockMap))
	}

	block.Codes = parseCodes(blockMap, debug, logger, ctx)

	if block.Name == "" && len(block.Codes) == 0 {
		return nil
	}

	return block
}

// parseCodes parses the codes within a block.
func parseCodes(blockMap map[string]any, debug bool, logger log.Logger, ctx context.Context) []*Code {
	codesRaw, ok := blockMap["codes"]
	if !ok {
		return nil
	}

	codesSlice, ok := codesRaw.([]any)
	if !ok {
		return nil
	}

	var codes []*Code
	for _, c := range codesSlice {
		if code := parseCode(c, debug, logger, ctx); code != nil {
			codes = append(codes, code)
		}
	}

	return codes
}

// parseCode parses a single code entry from a block.
func parseCode(c any, debug bool, logger log.Logger, ctx context.Context) *Code {
	codeMap := toStringMap(c)
	if codeMap == nil {
		return nil
	}

	code := &Code{}
	if name, ok := codeMap["name"].(string); ok {
		code.Name = name
	}

	if debug {
		logger.Debugf(ctx, "Code %s keys: %v", code.Name, maps.Keys(codeMap))
	}

	code.Script = parseScript(codeMap)

	if debug {
		logger.Debugf(ctx, "Code %s script length: %d", code.Name, len(code.Script))
	}

	if code.Name == "" && code.Script == "" {
		return nil
	}

	return code
}

// parseScript extracts the script content from a code map.
// Handles string, array of strings, and "scripts" (plural) field formats.
func parseScript(codeMap map[string]any) string {
	// Try "script" as string
	if script, ok := codeMap["script"].(string); ok {
		return script
	}

	// Try "script" as array of strings
	if scriptSlice, ok := codeMap["script"].([]any); ok {
		return joinScriptSlice(scriptSlice)
	}

	// Try "scripts" (plural) field
	if scripts, ok := codeMap["scripts"].([]any); ok {
		return joinScriptSlice(scripts)
	}

	return ""
}

// parseQueries parses the "queries" section for SQL transformations.
func parseQueries(params map[string]any) *CodeBlock {
	queriesRaw, ok := params["queries"]
	if !ok {
		return nil
	}

	queriesSlice, ok := queriesRaw.([]any)
	if !ok {
		return nil
	}

	block := &CodeBlock{Name: "queries"}
	for i, q := range queriesSlice {
		queryStr, ok := q.(string)
		if !ok {
			continue
		}
		block.Codes = append(block.Codes, &Code{
			Name:   fmt.Sprintf("query_%d", i+1),
			Script: queryStr,
		})
	}

	if len(block.Codes) == 0 {
		return nil
	}

	return block
}

// joinScriptSlice joins a slice of any to a string, filtering for strings only.
func joinScriptSlice(slice []any) string {
	var parts []string
	for _, s := range slice {
		if str, ok := s.(string); ok {
			parts = append(parts, str)
		}
	}
	return strings.Join(parts, "\n")
}

// ParseComponentConfig parses a Config into ComponentConfig.
func ParseComponentConfig(comp *keboola.ComponentWithConfigs, cfg *keboola.ConfigWithRows) *ComponentConfig {
	config := &ComponentConfig{
		ID:            cfg.ID.String(),
		Name:          cfg.Name,
		ComponentID:   comp.ID.String(),
		ComponentType: comp.Type,
		Description:   cfg.Description,
		IsDisabled:    cfg.IsDisabled,
		Version:       cfg.Version,
		Created:       cfg.Created.String(),
	}

	// Convert configuration content to map
	if cfg.Content != nil {
		config.Configuration = make(map[string]any)
		for _, key := range cfg.Content.Keys() {
			if val, ok := cfg.Content.Get(key); ok {
				config.Configuration[key] = val
			}
		}
	}

	return config
}
