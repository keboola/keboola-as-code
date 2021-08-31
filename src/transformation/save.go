package transformation

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/iancoleman/orderedmap"
	"github.com/otiai10/copy"
	"go.uber.org/zap"

	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/sql"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
)

type writer struct {
	projectDir  string
	logger      *zap.SugaredLogger
	naming      model.Naming
	componentId string
	errors      *utils.Error
}

// SaveBlocks - save code blocks from source config to the disk.
func SaveBlocks(projectDir string, logger *zap.SugaredLogger, naming model.Naming, config *model.ConfigManifest, source *model.Config) (*orderedmap.OrderedMap, error) {
	w := &writer{projectDir, logger, naming, source.ComponentId, utils.NewMultiError()}

	// Copy config content to remove blocks
	configContent := *source.Content

	// Load and clear "parameters.blocks" from the config
	var blocksRaw interface{} = nil
	if parametersRaw, found := configContent.Get(`parameters`); found {
		// Get blocks map
		parameters := parametersRaw.(orderedmap.OrderedMap)
		blocksRaw, _ = parameters.Get(`blocks`)

		// Don't save blocks to local config.json
		parameters.Delete(`blocks`)
		configContent.Set(`parameters`, parameters)
	}

	// Convert map to structs
	blocks := make([]*model.Block, 0)
	utils.ConvertByJson(blocksRaw, &blocks)

	// Fill in generated values
	blocksDir := w.naming.BlocksDir(config.RelativePath())
	blocksTmpDir := w.naming.BlocksTmpDir(config.RelativePath())
	for blockIndex, block := range blocks {
		block.ParentPath = blocksTmpDir
		block.Path = w.naming.BlockPath(blockIndex, block.Name)
		for codeIndex, code := range block.Codes {
			code.ParentPath = block.RelativePath()
			code.Path = w.naming.CodePath(codeIndex, code.Name)
			code.CodeFileName = w.naming.CodeFileName(config.ComponentId)
		}
	}

	// Write blocks to the disk
	w.writeBlocks(blocksDir, blocksTmpDir, blocks)

	return &configContent, w.errors.ErrorOrNil()
}

// writeBlocks to the temp dir, and if all ok move directory to the target path.
func (w *writer) writeBlocks(targetDir, tmpDir string, blocks []*model.Block) {
	blocksTmpDirAbs := filepath.Join(w.projectDir, tmpDir)

	// Create tmp dir, clear on the end
	if err := os.MkdirAll(blocksTmpDirAbs, 0755); err != nil {
		w.errors.Append(err)
		return
	}
	defer os.RemoveAll(blocksTmpDirAbs)

	// Blocks
	for _, block := range blocks {
		w.writeBlock(block)
	}

	// If no error, replace old dir with the new
	if w.errors.Len() == 0 {
		blocksDirAbs := filepath.Join(w.projectDir, targetDir)

		// Remove old content
		if err := os.RemoveAll(blocksDirAbs); err != nil {
			w.errors.Append(err)
		}

		// Copy new content to destination
		err := copy.Copy(blocksTmpDirAbs, blocksDirAbs, copy.Options{
			OnDirExists:   func(src, dest string) copy.DirExistsAction { return copy.Replace },
			Sync:          true,
			PreserveTimes: true,
		})
		if err != nil {
			w.errors.Append(err)
			return
		}
		w.logger.Debugf(`Moved "%s" -> "%s"`, tmpDir, targetDir)
	}
}

func (w *writer) writeBlock(block *model.Block) {
	// Validate
	if err := validator.Validate(block); err != nil {
		w.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid block \"%s\"`, block.RelativePath()), err))
		return
	}

	// Create dir
	blockDirAbs := filepath.Join(w.projectDir, block.RelativePath())
	if err := os.MkdirAll(blockDirAbs, 0755); err != nil {
		w.errors.Append(err)
		return
	}

	// Write metadata
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, block); metadata != nil {
		metaFilePath := w.naming.MetaFilePath(block.RelativePath())
		if err := json.WriteFile(w.projectDir, metaFilePath, metadata, "block metadata"); err == nil {
			w.logger.Debugf(`Saved "%s"`, metaFilePath)
		} else {
			w.errors.Append(err)
			return
		}
	}

	// Write codes
	for _, code := range block.Codes {
		w.writeCode(code)
	}
}

func (w *writer) writeCode(code *model.Code) {
	// Create dir
	codeDirAbs := filepath.Join(w.projectDir, code.RelativePath())
	if err := os.MkdirAll(codeDirAbs, 0755); err != nil {
		w.errors.Append(err)
		return
	}

	// Write metadata
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, code); metadata != nil {
		metaFilePath := w.naming.MetaFilePath(code.RelativePath())
		if err := json.WriteFile(w.projectDir, metaFilePath, metadata, "code metadata"); err == nil {
			w.logger.Debugf(`Saved "%s"`, metaFilePath)
		} else {
			w.errors.Append(err)
			return
		}
	}

	// Write scripts
	codePathRel := w.naming.CodeFilePath(code)
	codePathAbs := filepath.Join(w.projectDir, codePathRel)
	if err := os.WriteFile(codePathAbs, []byte(w.joinScripts(code.Scripts)), 0644); err != nil {
		w.errors.Append(err)
	}
}

func (w *writer) joinScripts(scripts []string) string {
	switch w.componentId {
	case `keboola.snowflake-transformation`:
		fallthrough
	case `keboola.synapse-transformation`:
		fallthrough
	case `keboola.oracle-transformation`:
		return sql.Join(scripts) + "\n"
	default:
		return strings.Join(scripts, "\n") + "\n"
	}
}
