package transformation

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation/blockcode"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/reflecthelper"
)

// MapBeforeLocalSave - save code blocks to the disk.
func (m *transformationMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.Object); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Create local writer
	w := &localWriter{
		State:           m.state,
		LocalSaveRecipe: recipe,
		config:          recipe.Object.(*model.Config),
		errors:          errors.NewMultiError(),
	}

	// Save using developer-friendly format (single file)
	return w.saveDeveloperFormat()
}

type localWriter struct {
	*state.State
	*model.LocalSaveRecipe
	config *model.Config
	errors errors.MultiError
}

// saveDeveloperFormat saves transformation code as a single file (transform.sql/transform.py).
func (w *localWriter) saveDeveloperFormat() error {
	if w.config.Transformation == nil {
		return nil
	}

	// Generate single transform file
	w.generateTransformFile()

	// Delete old blocks directory if it exists
	w.deleteOldBlocksDir()

	return w.errors.ErrorOrNil()
}

// generateTransformFile generates the single transform.sql/transform.py file.
func (w *localWriter) generateTransformFile() {
	blocks := w.config.Transformation.Blocks
	if len(blocks) == 0 {
		// Create empty transform file to preserve the transformation
		transformPath := w.NamingGenerator().TransformFilePath(w.Path(), w.config.ComponentID)
		w.Files.
			Add(filesystem.NewRawFile(transformPath, "")).
			SetDescription("transformation code").
			AddTag(model.FileTypeOther).
			AddTag(model.FileKindNativeCode)
		return
	}

	// Get shared codes map for expanding references
	sharedCodes := w.getSharedCodesMap()

	// Convert blocks to single string using blockcode package
	content := blockcode.BlocksToString(blocks, w.config.ComponentID, sharedCodes)

	// Write the transform file
	transformPath := w.NamingGenerator().TransformFilePath(w.Path(), w.config.ComponentID)
	w.Files.
		Add(filesystem.NewRawFile(transformPath, content)).
		SetDescription("transformation code").
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeCode)
}

// getSharedCodesMap returns a map of shared code IDs to their content.
func (w *localWriter) getSharedCodesMap() map[string]string {
	sharedCodes := make(map[string]string)
	// TODO: Populate from linked shared codes if needed
	// For now, shared codes will be referenced by their placeholder
	return sharedCodes
}

// deleteOldBlocksDir marks old blocks directory files for deletion.
func (w *localWriter) deleteOldBlocksDir() {
	blocksDir := w.NamingGenerator().BlocksDir(w.Path())

	// Delete all old files from blocks dir
	for _, path := range w.TrackedPaths() {
		if filesystem.IsFrom(path, blocksDir) && w.IsFile(path) {
			w.ToDelete = append(w.ToDelete, path)
		}
	}

	// Also delete the blocks directory itself if empty
	// The .gitkeep file will be removed as part of the tracked paths
}

// saveLegacyFormat saves transformation code using the legacy blocks/codes structure.
// This is kept for backward compatibility during the transition period.
func (w *localWriter) saveLegacyFormat() error {
	blocksDir := w.NamingGenerator().BlocksDir(w.Path())

	// Generate ".gitkeep" to preserve the "blocks" directory, even if there are no blocks.
	w.Files.
		Add(filesystem.NewRawFile(filesystem.Join(blocksDir, `.gitkeep`), ``)).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindGitKeep)

	// Generate files for blocks
	for _, block := range w.config.Transformation.Blocks {
		// Generate block files
		w.generateBlockFiles(block)
	}

	// Delete all old files from blocks dir
	// We always do full generation of blocks dir.
	for _, path := range w.TrackedPaths() {
		if filesystem.IsFrom(path, blocksDir) && w.IsFile(path) {
			w.ToDelete = append(w.ToDelete, path)
		}
	}

	return w.errors.ErrorOrNil()
}

func (w *localWriter) generateBlockFiles(block *model.Block) {
	// Validate
	if err := w.ValidateValue(block); err != nil {
		w.errors.AppendWithPrefixf(err, `invalid block \"%s\"`, block.Path())
		return
	}

	// Create metadata file
	if metadata := reflecthelper.MapFromTaggedFields(model.MetaFileFieldsTag, block); metadata != nil {
		metadataPath := w.NamingGenerator().MetaFilePath(block.Path())
		w.createMetadataFile(metadataPath, `block metadata`, model.FileKindBlockMeta, metadata)
	}

	// Create codes
	for _, code := range block.Codes {
		w.generateCodeFiles(code)
	}
}

func (w *localWriter) generateCodeFiles(code *model.Code) {
	// Create metadata file
	if metadata := reflecthelper.MapFromTaggedFields(model.MetaFileFieldsTag, code); metadata != nil {
		metadataPath := w.NamingGenerator().MetaFilePath(code.Path())
		w.createMetadataFile(metadataPath, `code metadata`, model.FileKindCodeMeta, metadata)
	}

	// Determine code file name
	codeFileName := code.CodeFileName
	if codeFileName == "" {
		codeFileName = naming.CodeFileName + "." + naming.CodeFileExt(code.ComponentID)
	}

	// Create code file
	w.Files.
		Add(filesystem.NewRawFile(filesystem.Join(code.Path(), codeFileName), code.Scripts.String(code.ComponentID))).
		SetDescription(`code`).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeCode)
}

func (w *localWriter) createMetadataFile(path, desc, tag string, content *orderedmap.OrderedMap) {
	w.Files.
		Add(filesystem.NewJSONFile(path, content)).
		SetDescription(desc).
		AddTag(model.FileTypeJSON).
		AddTag(tag)
}
