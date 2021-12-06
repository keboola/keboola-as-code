package transformation

import (
	"fmt"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

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
	errors *utils.MultiError
}

func (w *localWriter) save() error {
	blocksDir := w.Naming.BlocksDir(w.ObjectManifest.Path())

	// Generate ".gitkeep" to preserve the "blocks" directory, even if there are no blocks.
	w.Files.
		Add(filesystem.NewFile(filesystem.Join(blocksDir, `.gitkeep`), ``)).
		AddTag(model.FileTypeOther)

	// Generate files for blocks
	for _, block := range w.config.Blocks {
		// Generate block files
		w.generateBlockFiles(block)
	}

	// Delete all old files from blocks dir
	// We always do full generation of blocks dir.
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
	if metadata := utils.MapFromTaggedFields(model.MetaFileFieldsTag, block); metadata != nil {
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
	if metadata := utils.MapFromTaggedFields(model.MetaFileFieldsTag, code); metadata != nil {
		metadataPath := w.Naming.MetaFilePath(code.Path())
		w.createMetadataFile(metadataPath, `code metadata`, metadata)
	}

	// Create code file
	w.Files.
		Add(filesystem.NewFile(w.Naming.CodeFilePath(code), code.ScriptsToString()).SetDescription(`code`)).
		AddTag(model.FileTypeNativeCode)
}

func (w *localWriter) createMetadataFile(path, desc string, content *orderedmap.OrderedMap) {
	w.Files.
		Add(filesystem.NewJsonFile(path, content).SetDescription(desc)).
		AddTag(model.MetaFile).
		AddTag(model.FileTypeJson)
}
