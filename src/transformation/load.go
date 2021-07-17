package transformation

import (
	"fmt"
	"github.com/iancoleman/orderedmap"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/sql"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
	"strings"
)

type loader struct {
	projectDir  string
	naming      *manifest.LocalNaming
	componentId string
}

// LoadBlocks - load code blocks from disk to target config
func LoadBlocks(projectDir string, naming *manifest.LocalNaming, record *manifest.ConfigManifest, target *model.Config) error {
	l := &loader{projectDir, naming, target.ComponentId}
	blocksDir := filepath.Join(record.RelativePath(), manifest.TransformationBlocksDir)
	blocks, err := l.loadBlocks(blocksDir)
	if err != nil {
		return err
	}

	// Set blocks to "parameters.blocks" in the config
	var parameters orderedmap.OrderedMap
	if parametersRaw, found := target.Content.Get(`parameters`); found {
		parameters = parametersRaw.(orderedmap.OrderedMap)
	} else {
		parameters = *utils.NewOrderedMap()
	}

	// Convert []struct to []map
	blocksMap := make([]interface{}, 0)
	for _, block := range blocks {
		blockMap := utils.NewOrderedMap()
		utils.ConvertByJson(block, &blockMap)
		blocksMap = append(blocksMap, blockMap)
	}
	parameters.Set("blocks", blocksMap)
	target.Content.Set("parameters", parameters)
	target.Blocks = blocks
	return nil
}

// loadBlocks - one block is one dir from blocksDir
func (l *loader) loadBlocks(blocksDir string) (blocks []*model.Block, err error) {
	blocks = make([]*model.Block, 0)
	errors := utils.NewMultiError()
	blocksDirAbs := filepath.Join(l.projectDir, blocksDir)

	// Check if blocks dir exists
	if !utils.IsDir(blocksDirAbs) {
		errors.Append(fmt.Errorf(`missing blocks dir "%s"`, blocksDir))
		return nil, errors
	}

	// Load all dir entries
	items, err := os.ReadDir(blocksDirAbs)
	if err != nil {
		errors.Append(fmt.Errorf(`cannot read transformation blocks from "%s": %s`, blocksDir, err.Error()))
		return nil, errors
	}

	// Load all blocks
	for _, item := range items {
		if item.IsDir() {
			block := &model.Block{
				ParentPath: blocksDir,
				Path:       item.Name(),
			}

			// Load meta file
			errPrefix := "block metadata"
			if err := utils.ReadTaggedFields(l.projectDir, block.MetaFilePath(), model.MetaFileTag, errPrefix, block); err != nil {
				errors.Append(err)
			}

			// Load codes
			codes, err := l.loadCodes(block.RelativePath())
			if err == nil {
				block.Codes = codes
			} else {
				errors.Append(err)
				continue
			}

			// Validate
			if errors.Len() == 0 {
				if err := validator.Validate(block); err != nil {
					errors.Append(utils.PrefixError(fmt.Sprintf(`block "%s" is not valid`, block.RelativePath()), err))
				}
			}

			// Store
			blocks = append(blocks, block)
		}
	}

	return blocks, errors.ErrorOrNil()
}

// loadCodes - one code is one dir from block dir
func (l *loader) loadCodes(blockDir string) (codes []*model.Code, err error) {
	codes = make([]*model.Code, 0)
	errors := utils.NewMultiError()
	blockDirAbs := filepath.Join(l.projectDir, blockDir)

	// Load all dir entries
	items, err := os.ReadDir(blockDirAbs)
	if err != nil {
		errors.Append(fmt.Errorf(`cannot read transformation codes from "%s": %s`, blockDirAbs, err.Error()))
		return nil, errors
	}

	// Load all blocks
	for _, item := range items {
		if item.IsDir() {
			code := &model.Code{
				ParentPath: blockDir,
				Path:       item.Name(),
				Extension:  l.naming.CodeFileExt(l.componentId),
			}

			// Load meta file
			errPrefix := "code metadata"
			if err := utils.ReadTaggedFields(l.projectDir, code.MetaFilePath(), model.MetaFileTag, errPrefix, code); err != nil {
				errors.Append(err)
			}

			// Load codes
			scripts, err := l.loadScripts(code.CodeFilePath())
			if err == nil {
				code.Scripts = scripts
			} else {
				errors.Append(err)
				continue
			}

			// Store
			codes = append(codes, code)
		}
	}

	return codes, errors.ErrorOrNil()
}

// loadScripts - one script is one statement from code file
func (l *loader) loadScripts(codeFilePath string) (scripts []string, err error) {
	codeFilePathAbs := filepath.Join(l.projectDir, codeFilePath)

	// Check if file exists
	if !utils.IsFile(codeFilePathAbs) {
		return nil, fmt.Errorf(`missing code file "%s"`, codeFilePath)
	}

	// Load file content
	content, err := os.ReadFile(codeFilePathAbs)
	if err != nil {
		return nil, fmt.Errorf(`cannot read code file "%s": %s`, codeFilePath, err)
	}

	// Split to scripts
	scripts = l.parseScripts(string(content))
	return scripts, nil
}

func (l *loader) parseScripts(content string) []string {
	switch l.componentId {
	case `keboola.snowflake-transformation`:
		fallthrough
	case `keboola.synapse-transformation`:
		fallthrough
	case `keboola.oracle-transformation`:
		return sql.Split(content)
	default:
		return []string{strings.TrimSuffix(content, "\n")}
	}
}
