package transformation

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type localSaveContext struct {
	*model.LocalSaveRecipe
	state          *local.State
	transformation *model.Config
	basePath       model.AbsPath
}

// MapBeforeLocalSave - save code blocks to the disk.
func (m *transformationLocalMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformation(recipe.Object.Key()); err != nil {
		return err
	} else if !ok {
		return nil
	}
	transformation := recipe.Object.(*model.Config)

	basePath, err := m.state.GetPath(transformation)
	if err != nil {
		return err
	}

	// Create local writer
	w := &localSaveContext{
		state:           m.state,
		LocalSaveRecipe: recipe,
		transformation:  transformation,
		basePath:        basePath,
	}

	// Save
	return w.save()
}

func (c *localSaveContext) save() error {
	blocksDir := c.state.NamingGenerator().BlocksDir(c.basePath)

	// Generate ".gitkeep" to preserve the "blocks" directory, even if there are no blocks.
	c.Files.
		Add(filesystem.NewRawFile(filesystem.Join(blocksDir.String(), `.gitkeep`), ``)).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindGitKeep)

	// Init value if needed
	if c.transformation.Transformation == nil {
		c.transformation.Transformation = &model.Transformation{}
	}

	// Generate files for blocks
	errors := utils.NewMultiError()
	for _, block := range c.transformation.Transformation.Blocks {
		// Generate block files
		if err := c.saveBlock(block); err != nil {
			errors.Append(err)
		}
	}

	// Delete all old files from blocks dir
	// We always do full generation of blocks dir.
	fs := c.state.ObjectsRoot()
	for _, path := range c.state.TrackedPaths() {
		if filesystem.IsFrom(path, blocksDir.String()) && fs.IsFile(path) {
			c.ToDelete = append(c.ToDelete, path)
		}
	}

	return errors.ErrorOrNil()
}

func (c *localSaveContext) saveBlock(block *model.Block) error {
	// Validate
	if err := validator.Validate(c.state.Ctx(), block); err != nil {
		return utils.PrefixError(fmt.Sprintf(`invalid block \"%s\"`, block.String()), err)
	}

	// Get path
	blockDir, err := c.state.GetPath(block)
	if err != nil {
		return err
	}

	// Create metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileFieldsTag, block); metadata != nil {
		metadataPath := c.state.NamingGenerator().MetaFilePath(blockDir)
		c.createMetadataFile(metadataPath, `block metadata`, model.FileKindBlockMeta, metadata)
	}

	// Generate codes
	errors := utils.NewMultiError()
	for _, code := range block.Codes {
		if err := c.saveCode(code); err != nil {
			errors.Append(err)
		}
	}
	return errors.ErrorOrNil()
}

func (c *localSaveContext) saveCode(code *model.Code) error {
	// Get path
	codeDir, err := c.state.GetPath(code)
	if err != nil {
		return err
	}

	// Create metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileFieldsTag, code); metadata != nil {
		metadataPath := c.state.NamingGenerator().MetaFilePath(codeDir)
		c.createMetadataFile(metadataPath, `code metadata`, model.FileKindCodeMeta, metadata)
	}

	codeFileName := c.state.NamingGenerator().CodeFileName(code.ComponentId())
	codeFilePath := filesystem.Join(codeDir.String(), codeFileName)

	// Create code file
	c.Files.
		Add(filesystem.NewRawFile(codeFilePath, code.Scripts.String(code.ComponentId()))).
		SetDescription(`code`).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeCode)

	return nil
}

func (c *localSaveContext) createMetadataFile(path, desc, tag string, content *orderedmap.OrderedMap) {
	c.Files.
		Add(filesystem.NewJsonFile(path, content)).
		SetDescription(desc).
		AddTag(model.FileTypeJson).
		AddTag(tag)
}
