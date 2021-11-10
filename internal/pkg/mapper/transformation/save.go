package transformation

import (
	"fmt"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// MapBeforeRemoteSave - save code blocks to the API.
func (m *transformationMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.InternalObject); err != nil {
		return err
	} else if !ok {
		return nil
	}
	internalObject := recipe.InternalObject.(*model.Config)
	apiObject := recipe.ApiObject.(*model.Config)

	// Get parameters
	var parameters orderedmap.OrderedMap
	parametersRaw := utils.GetFromMap(apiObject.Content, []string{`parameters`})
	if v, ok := parametersRaw.(orderedmap.OrderedMap); ok {
		parameters = v
	} else {
		parameters = *utils.NewOrderedMap()
	}

	// Convert blocks to map
	blocks := make([]interface{}, 0)
	for _, block := range internalObject.Blocks {
		blockRaw := *utils.NewOrderedMap()
		if err := utils.ConvertByJson(block, &blockRaw); err != nil {
			return fmt.Errorf(`cannot convert block to JSON: %w`, err)
		}
		blocks = append(blocks, blockRaw)
	}

	// Add "parameters.blocks" to configuration content
	parameters.Set("blocks", blocks)

	// Set parameters
	apiObject.Content.Set(`parameters`, parameters)

	// Clear blocks in API object
	apiObject.Blocks = nil

	// Update changed fields
	if recipe.ChangedFields.Has(`blocks`) {
		recipe.ChangedFields.Remove(`blocks`)
		recipe.ChangedFields.Add(`configuration`)
	}

	return nil
}

// MapBeforeLocalSave - save code blocks to the disk.
func (m *transformationMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.Object); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Create local writer
	w := &localWriter{
		MapperContext:   m.MapperContext,
		LocalSaveRecipe: recipe,
		config:          recipe.Object.(*model.Config),
		errors:          utils.NewMultiError(),
	}

	// Save
	return w.save()
}

type localWriter struct {
	model.MapperContext
	*model.LocalSaveRecipe
	config *model.Config
	errors *utils.Error
}

func (w *localWriter) save() error {
	// Generate files for blocks
	for _, block := range w.config.Blocks {
		// Generate block files
		w.generateBlockFiles(block)
	}

	// Delete all old files from blocks dir
	// We always do full generation of blocks dir.
	blocksDir := w.Naming.BlocksDir(w.Record.Path())
	for _, path := range w.State.TrackedPaths() {
		if filesystem.IsFrom(path, blocksDir) && w.State.IsFile(path) {
			w.ToDelete = append(w.ToDelete, path)
		}
	}

	return w.errors.ErrorOrNil()
}

func (w *localWriter) generateBlockFiles(block *model.Block) {
	// Validate
	if err := validator.Validate(block); err != nil {
		w.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid block \"%s\"`, block.Path()), err))
		return
	}

	// Create metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, block); metadata != nil {
		metadataPath := w.Naming.MetaFilePath(block.Path())
		w.createMetadataFile(metadataPath, `block metadata`, metadata)
	}

	// Create codes
	for _, code := range block.Codes {
		w.generateCodeFiles(code)
	}
}

func (w *localWriter) generateCodeFiles(code *model.Code) {
	// Create metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, code); metadata != nil {
		metadataPath := w.Naming.MetaFilePath(code.Path())
		w.createMetadataFile(metadataPath, `code metadata`, metadata)
	}

	// Create code file
	file := filesystem.
		CreateFile(w.Naming.CodeFilePath(code), code.ScriptsToString()).
		SetDescription(`code`)
	w.ExtraFiles = append(w.ExtraFiles, file)
}

func (w *localWriter) createMetadataFile(path, desc string, content *orderedmap.OrderedMap) {
	file, err := filesystem.
		CreateJsonFile(path, content).
		SetDescription(desc).
		ToFile()
	if err == nil {
		w.ExtraFiles = append(w.ExtraFiles, file)
	} else {
		w.errors.Append(err)
	}
}
