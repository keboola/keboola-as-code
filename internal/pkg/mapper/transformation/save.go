package transformation

import (
	"fmt"
	"os"
	"strings"

	"github.com/iancoleman/orderedmap"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/sql"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type writer struct {
	*files
	logger    *zap.SugaredLogger
	fs        filesystem.Fs
	naming    model.Naming
	state     *model.State
	config    *model.Config
	configDir string
	errors    *utils.Error
}

// Save - save code blocks from source config to the disk.
func Save(logger *zap.SugaredLogger, fs filesystem.Fs, naming model.Naming, state *model.State, files *model.ObjectFiles) error {
	w := &writer{
		files:     files,
		logger:    logger,
		fs:        fs,
		naming:    naming,
		state:     state,
		config:    files.Object.(*model.Config),
		configDir: files.Record.Path(),
		errors:    utils.NewMultiError(),
	}
	return w.save()
}

func (w *writer) save() error {
	// Load and clear "parameters.blocks" from the record
	var blocksRaw interface{} = nil
	if parametersRaw, found := w.Configuration.Content.Get(`parameters`); found {
		// Get blocks map
		parameters := parametersRaw.(orderedmap.OrderedMap)
		blocksRaw, _ = parameters.Get(`blocks`)

		// Remove blocks from config.json
		parameters.Delete(`blocks`)
		w.Configuration.Content.Set(`parameters`, parameters)
	}

	// Convert map to structs
	blocks := make([]*model.Block, 0)
	utils.ConvertByJson(blocksRaw, &blocks)

	// Fill in values AND generate files
	blocksDir := w.naming.BlocksDir(w.configDir)
	for blockIndex, block := range blocks {
		block.BranchId = w.config.BranchId
		block.ComponentId = w.config.ComponentId
		block.ConfigId = w.config.Id
		block.Index = blockIndex
		block.PathInProject = w.naming.BlockPath(blocksDir, block)
		for codeIndex, code := range block.Codes {
			code.BranchId = w.config.BranchId
			code.ComponentId = w.config.ComponentId
			code.ConfigId = w.config.Id
			code.Index = codeIndex
			code.PathInProject = w.naming.CodePath(block.Path(), code)
			code.CodeFileName = w.naming.CodeFileName(w.config.ComponentId)
		}

		// Generate block files
		w.generateBlockFiles(block)
	}

	// Delete all old files from blocks dir
	// We always do full generation of blocks dir.
	blocksDirWithSep := blocksDir + string(os.PathSeparator)
	for _, path := range w.state.TrackedPaths() {
		if strings.HasPrefix(path, blocksDirWithSep) && w.state.IsFile(path) {
			w.ToDelete = append(w.ToDelete, path)
		}
	}

	return w.errors.ErrorOrNil()
}

func (w *writer) generateBlockFiles(block *model.Block) {
	// Validate
	if err := validator.Validate(block); err != nil {
		w.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid block \"%s\"`, block.Path()), err))
		return
	}

	// Create metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, block); metadata != nil {
		metadataPath := w.naming.MetaFilePath(block.Path())
		w.createMetadataFile(metadataPath, `block metadata`, metadata)
	}

	// Create codes
	for _, code := range block.Codes {
		w.generateCodeFiles(code)
	}
}

func (w *writer) generateCodeFiles(code *model.Code) {
	// Create metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, code); metadata != nil {
		metadataPath := w.naming.MetaFilePath(code.Path())
		w.createMetadataFile(metadataPath, `code metadata`, metadata)
	}

	// Create code file
	file := filesystem.
		CreateFile(w.naming.CodeFilePath(code), w.joinScripts(code.Scripts)).
		SetDescription(`code`)
	w.Extra = append(w.Extra, file)
}

func (w *writer) createMetadataFile(path, desc string, content *orderedmap.OrderedMap) {
	file, err := filesystem.
		CreateJsonFile(path, content).
		SetDescription(desc).
		ToFile()
	if err == nil {
		w.Extra = append(w.Extra, file)
	} else {
		w.errors.Append(err)
	}
}

func (w *writer) joinScripts(scripts []string) string {
	switch w.config.ComponentId {
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
