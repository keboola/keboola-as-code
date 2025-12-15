package local

import (
	"fmt"
	"slices"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Manager) createObject(key model.Key, name string) (model.Object, error) {
	switch k := key.(type) {
	case model.ConfigKey:
		component, err := m.state.Components().GetOrErr(k.ComponentID)
		if err != nil {
			return nil, err
		}
		content, err := generateContent(component.Schema, component.EmptyConfig)
		if err != nil {
			return nil, err
		}
		config := &model.Config{
			ConfigKey: k,
			Name:      name,
			Content:   content,
		}
		if component.IsTransformationWithBlocks() {
			config.Transformation = &model.Transformation{}
			// Ensure at least one block and code for transformations with blocks
			ensureMinimalBlocks(config)
		}
		if component.IsOrchestrator() {
			config.Orchestration = &model.Orchestration{}
		}
		return config, nil
	case model.ConfigRowKey:
		component, err := m.state.Components().GetOrErr(k.ComponentID)
		if err != nil {
			return nil, err
		}
		content, err := generateContent(component.SchemaRow, component.EmptyConfigRow)
		if err != nil {
			return nil, err
		}
		return &model.ConfigRow{
			ConfigRowKey: k,
			Name:         name,
			Content:      content,
		}, nil
	default:
		panic(errors.Errorf(`unexpected type "%T"`, key))
	}
}

func generateContent(schemaDef []byte, defaultConfig *orderedmap.OrderedMap) (*orderedmap.OrderedMap, error) {
	finalContent := orderedmap.New()
	// Use default configuration if defined in the component's metadata
	if len(defaultConfig.Keys()) > 0 {
		if slices.Contains(defaultConfig.Keys(), "parameters") {
			return defaultConfig, nil
		}
		finalContent.Set("parameters", defaultConfig)
		return finalContent, nil
	}

	// Otherwise, generate configuration from the JSON schema
	content, err := schema.GenerateDocument(schemaDef)
	if err != nil {
		return nil, err
	}

	// wrap config content to parameters
	// { "parameters":{...}
	if content.Len() != 0 {
		finalContent.Set("parameters", content)
	}
	return finalContent, nil
}

// ensureMinimalBlocks ensures that transformations with blocks have at least one block and one code.
// This is necessary because the schema might generate empty arrays, but transformations need at least one block to work.
func ensureMinimalBlocks(config *model.Config) {
	parameters, _, _ := config.Content.GetNestedMap(`parameters`)
	if parameters == nil {
		parameters = orderedmap.New()
		config.Content.Set(`parameters`, parameters)
	}

	// Check if blocks exist and if not, or if empty, create at least one
	var blocks []any
	blocksRaw, hasBlocks := parameters.Get(`blocks`)
	if hasBlocks {
		if v, ok := blocksRaw.([]any); ok {
			blocks = v
		}
	}

	// If blocks is empty, add one block with one code
	if len(blocks) == 0 {
		block := orderedmap.New()
		block.Set("name", "Block 1")

		code := orderedmap.New()
		code.Set("name", "Code")
		code.Set("script", []any{""})

		block.Set("codes", []any{code})
		blocks = []any{block}
		parameters.Set("blocks", blocks)
	} else {
		// Ensure each block has at least one code and proper names
		for blockIndex, blockRaw := range blocks {
			if blockMap, ok := blockRaw.(*orderedmap.OrderedMap); ok {
				// Ensure block has a name
				if name, hasName := blockMap.Get("name"); !hasName || name == "" {
					blockMap.Set("name", fmt.Sprintf("Block %d", blockIndex+1))
				}

				var codes []any
				codesRaw, hasCodes := blockMap.Get(`codes`)
				if hasCodes {
					if v, ok := codesRaw.([]any); ok {
						codes = v
					}
				}

				// If codes is empty, add one code with default name
				if len(codes) == 0 {
					code := orderedmap.New()
					code.Set("name", "Code")
					code.Set("script", []any{""})
					codes = []any{code}
					blockMap.Set("codes", codes)
				} else {
					// Ensure each code has a name
					for codeIndex, codeRaw := range codes {
						if codeMap, ok := codeRaw.(*orderedmap.OrderedMap); ok {
							if name, hasName := codeMap.Get("name"); !hasName || name == "" {
								if codeIndex == 0 {
									codeMap.Set("name", "Code")
								} else {
									codeMap.Set("name", fmt.Sprintf("Code %d", codeIndex+1))
								}
							}
						}
					}
				}
			}
		}
	}
}
