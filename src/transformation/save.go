package transformation

import (
	"fmt"
	"github.com/iancoleman/orderedmap"
	"github.com/otiai10/copy"
	"go.uber.org/zap"
	"keboola-as-code/src/json"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/sql"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
	"strings"
)

type writer struct {
	projectDir  string
	logger      *zap.SugaredLogger
	naming      *model.Naming
	componentId string
	errors      *utils.Error
}

// SaveBlocks - save code blocks from source config to the disk
func SaveBlocks(projectDir string, logger *zap.SugaredLogger, naming *model.Naming, record *manifest.ConfigManifest, source *model.Config) (*orderedmap.OrderedMap, error) {
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

	// Write blocks to the disk
	w.writeBlocks(record.RelativePath(), blocks)

	return &configContent, w.errors.ErrorOrNil()
}

func (w *writer) writeBlocks(configDir string, blocks []*model.Block) {
	blocksTmpDir := w.naming.BlocksTmpDir(configDir)
	blocksTmpDirAbs := filepath.Join(w.projectDir, blocksTmpDir)

	// Create tmp dir, clear on the end
	if err := os.MkdirAll(blocksTmpDirAbs, 0755); err != nil {
		w.errors.Append(err)
		return
	}
	defer os.RemoveAll(blocksTmpDirAbs)

	// Blocks
	for index, block := range blocks {
		blockDir := filepath.Join(blocksTmpDir, w.naming.BlockPath(index, block.Name))
		w.writeBlock(blockDir, block)
	}

	// If no error, replace old dir with the new
	if w.errors.Len() == 0 {
		blocksDir := w.naming.BlocksDir(configDir)
		blocksDirAbs := filepath.Join(w.projectDir, blocksDir)

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
		w.logger.Debugf(`Moved "%s" -> "%s"`, blocksTmpDir, blocksDir)
	}
}

func (w *writer) writeBlock(blockDir string, block *model.Block) {
	// Validate
	if err := validator.Validate(block); err != nil {
		w.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid block \"%s\"`, blockDir), err))
		return
	}

	// Create dir
	blockDirAbs := filepath.Join(w.projectDir, blockDir)
	if err := os.MkdirAll(blockDirAbs, 0755); err != nil {
		w.errors.Append(err)
		return
	}

	// Write metadata
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, block); metadata != nil {
		metaFilePath := filepath.Join(blockDir, model.MetaFile)
		if err := json.WriteFile(w.projectDir, metaFilePath, metadata, "block metadata"); err == nil {
			w.logger.Debugf(`Saved "%s"`, metaFilePath)
		} else {
			w.errors.Append(err)
			return
		}
	}

	// Write codes
	for index, code := range block.Codes {
		codeDir := filepath.Join(blockDir, w.naming.CodePath(index, code.Name))
		w.writeCode(codeDir, code)
	}
}

func (w *writer) writeCode(codeDir string, code *model.Code) {
	// Create dir
	codeDirAbs := filepath.Join(w.projectDir, codeDir)
	if err := os.MkdirAll(codeDirAbs, 0755); err != nil {
		w.errors.Append(err)
		return
	}

	// Write metadata
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, code); metadata != nil {
		metaFilePath := filepath.Join(codeDir, model.MetaFile)
		if err := json.WriteFile(w.projectDir, metaFilePath, metadata, "code metadata"); err == nil {
			w.logger.Debugf(`Saved "%s"`, metaFilePath)
		} else {
			w.errors.Append(err)
			return
		}
	}

	// Write scripts
	codePathRel := filepath.Join(codeDir, w.naming.CodeFileName(w.componentId))
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
