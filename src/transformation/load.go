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
	errors      *utils.Error
}

// LoadBlocks - load code blocks from disk to target config
func LoadBlocks(projectDir string, naming *manifest.LocalNaming, record *manifest.ConfigManifest, target *model.Config) error {
	l := &loader{projectDir, naming, target.ComponentId, utils.NewMultiError()}
	blocks := l.loadBlocks(filepath.Join(record.RelativePath(), blocksDir))

	// Set blocks to "parameters.blocks" in the config
	var parameters orderedmap.OrderedMap
	if parametersRaw, found := target.Content.Get(`parameters`); found {
		// Use existing map
		parameters = parametersRaw.(orderedmap.OrderedMap)
	} else {
		// Create new map if not exists
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
	return l.errors.ErrorOrNil()
}

// loadBlocks - one block is one dir from blocksDir
func (l *loader) loadBlocks(blocksDir string) []*model.Block {
	blocks := make([]*model.Block, 0)
	blocksDirAbs := filepath.Join(l.projectDir, blocksDir)

	// Check if blocks dir exists
	if !utils.IsDir(blocksDirAbs) {
		l.errors.Append(fmt.Errorf(`missing blocks dir "%s"`, blocksDir))
		return nil
	}

	// Load all dir entries
	items, err := os.ReadDir(blocksDirAbs)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read transformation blocks from "%s": %s`, blocksDir, err.Error()))
		return nil
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
				l.errors.Append(err)
			}

			// Load codes
			codes := l.loadCodes(block.RelativePath())
			if codes != nil {
				block.Codes = codes
			} else {
				continue
			}

			// Store
			blocks = append(blocks, block)
		}
	}

	// Validate
	if l.errors.Len() == 0 {
		for _, block := range blocks {
			if err := validator.Validate(block); err != nil {
				l.errors.Append(utils.PrefixError(fmt.Sprintf(`block "%s" is not valid`, block.RelativePath()), err))
			}
		}
	}

	return blocks
}

// loadCodes - one code is one dir from block dir
func (l *loader) loadCodes(blockDir string) []*model.Code {
	codes := make([]*model.Code, 0)
	blockDirAbs := filepath.Join(l.projectDir, blockDir)

	// Load all dir entries
	items, err := os.ReadDir(blockDirAbs)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read transformation codes from "%s": %s`, blockDirAbs, err.Error()))
		return nil
	}

	// Load all codes
	for _, item := range items {
		if item.IsDir() {
			code := &model.Code{
				ParentPath: blockDir,
				Path:       item.Name(),
			}

			// Load meta file
			errPrefix := "code metadata"
			if err := utils.ReadTaggedFields(l.projectDir, code.MetaFilePath(), model.MetaFileTag, errPrefix, code); err != nil {
				l.errors.Append(err)
			}

			// Load codes
			code.CodeFileName = l.codeFileName(code.RelativePath())
			if code.CodeFileName != "" {
				scripts := l.loadScripts(code.CodeFilePath())
				if scripts != nil {
					code.Scripts = scripts
				} else {
					continue
				}
			}

			// Store
			codes = append(codes, code)
		}
	}

	return codes
}

func (l *loader) codeFileName(codeDir string) string {
	// Search for code file, glob "code.*"
	// File can use an old naming, so the file extension is not specified
	codeDirAbs := filepath.Join(l.projectDir, codeDir)
	matches, err := filepath.Glob(filepath.Join(codeDirAbs, model.CodeFileName+`.*`))
	if err != nil {
		l.errors.Append(fmt.Errorf(`canoot search for code file in %s": %s`, codeDir, err))
		return ""
	}
	files := make([]string, 0)
	for _, match := range matches {
		if utils.IsFile(match) {
			files = append(files, utils.RelPath(codeDirAbs, match))
		}
	}

	// No file?
	if len(files) == 0 {
		l.errors.Append(fmt.Errorf(`missing code file in "%s"`, codeDir))
		return ""
	}

	// Multiple files?
	if len(files) > 1 {
		l.errors.Append(fmt.Errorf(
			`expected one, but found multiple code files "%s" in "%s"`,
			strings.Join(files, `", "`),
			codeDir,
		))
		return ""
	}

	// Found
	return files[0]
}

// loadScripts - one script is one statement from code file
func (l *loader) loadScripts(codeFile string) []string {
	// Load file content
	codeFilePathAbs := filepath.Join(l.projectDir, codeFile)
	content, err := os.ReadFile(codeFilePathAbs)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read code file "%s": %s`, codeFile, err))
		return nil
	}

	// Split to scripts
	return l.parseScripts(string(content))
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
