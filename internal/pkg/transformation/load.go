package transformation

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/iancoleman/orderedmap"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/sql"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type loader struct {
	projectDir string
	logger     *zap.SugaredLogger
	naming     model.Naming
	record     *model.ConfigManifest
	target     *model.Config
	errors     *utils.Error
}

// LoadBlocks - load code blocks from disk to target config.
func LoadBlocks(projectDir string, logger *zap.SugaredLogger, naming model.Naming, record *model.ConfigManifest, target *model.Config) error {
	l := &loader{projectDir, logger, naming, record, target, utils.NewMultiError()}
	return l.loadBlocksToConfig()
}

// LoadBlocks - load code blocks from disk to target config.
func (l *loader) loadBlocksToConfig() error {
	// Load blocks
	blocksDir := l.naming.BlocksDir(l.record.RelativePath())
	blocks := l.loadBlocks(blocksDir)

	// Set blocks to "parameters.blocks" in the config
	var parameters orderedmap.OrderedMap
	if parametersRaw, found := l.target.Content.Get(`parameters`); found {
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
	l.target.Content.Set("parameters", parameters)
	l.target.Blocks = blocks
	return l.errors.ErrorOrNil()
}

// loadBlocks - one block is one dir from blocksDir.
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
		l.errors.Append(fmt.Errorf(`cannot read transformation blocks from "%s": %w`, blocksDir, err))
		return nil
	}

	// Load all blocks
	for i, item := range items {
		if item.IsDir() {
			// Create block struct
			block := &model.Block{
				BlockKey: model.BlockKey{
					BranchId:    l.target.BranchId,
					ComponentId: l.target.ComponentId,
					ConfigId:    l.target.Id,
					Index:       i,
				},
				Paths: model.Paths{PathInProject: model.PathInProject{ParentPath: blocksDir, ObjectPath: item.Name()}},
			}
			l.record.AddRelatedPath(block.RelativePath())

			// Load meta file
			metaFilePath := l.naming.MetaFilePath(block.RelativePath())
			errPrefix := "block metadata"
			if err := utils.ReadTaggedFields(l.projectDir, metaFilePath, model.MetaFileTag, block, errPrefix); err == nil {
				l.record.AddRelatedPath(metaFilePath)
				l.logger.Debugf(`Loaded "%s"`, metaFilePath)
			} else {
				l.errors.Append(err)
			}

			// Load codes
			codes := l.loadCodes(block)
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

// loadCodes - one code is one dir from block dir.
func (l *loader) loadCodes(block *model.Block) []*model.Code {
	codes := make([]*model.Code, 0)
	blockDirAbs := filepath.Join(l.projectDir, block.RelativePath())

	// Load all dir entries
	items, err := os.ReadDir(blockDirAbs)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read transformation codes from "%s": %w`, blockDirAbs, err))
		return nil
	}

	// Load all codes
	for i, item := range items {
		if item.IsDir() {
			// Create code struct
			code := &model.Code{
				CodeKey: model.CodeKey{
					BranchId:    l.target.BranchId,
					ComponentId: l.target.ComponentId,
					ConfigId:    l.target.Id,
					BlockIndex:  block.Index,
					Index:       i,
				},
				Paths: model.Paths{PathInProject: model.PathInProject{ParentPath: block.RelativePath(), ObjectPath: item.Name()}},
			}
			l.record.AddRelatedPath(code.RelativePath())

			// Load meta file
			metaFilePath := l.naming.MetaFilePath(code.RelativePath())
			errPrefix := "code metadata"
			if err := utils.ReadTaggedFields(l.projectDir, metaFilePath, model.MetaFileTag, code, errPrefix); err == nil {
				l.record.AddRelatedPath(metaFilePath)
				l.logger.Debugf(`Loaded "%s"`, metaFilePath)
			} else {
				l.errors.Append(err)
			}

			// Load codes
			code.CodeFileName = l.codeFileName(code.RelativePath())
			codeFilePath := l.naming.CodeFilePath(code)
			if code.CodeFileName != "" {
				scripts := l.loadScripts(codeFilePath)
				if scripts != nil {
					l.record.AddRelatedPath(codeFilePath)
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
		l.errors.Append(fmt.Errorf(`canoot search for code file in %s": %w`, codeDir, err))
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

// loadScripts - one script is one statement from code file.
func (l *loader) loadScripts(codeFile string) []string {
	// Load file content
	codeFilePathAbs := filepath.Join(l.projectDir, codeFile)
	content, err := os.ReadFile(codeFilePathAbs)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read code file "%s": %w`, codeFile, err))
		return nil
	}

	// Split to scripts
	scripts := l.parseScripts(string(content))
	l.logger.Debugf(`Loaded "%s", parsed "%d" scripts`, codeFile, len(scripts))
	return scripts
}

func (l *loader) parseScripts(content string) []string {
	switch l.record.ComponentId {
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
