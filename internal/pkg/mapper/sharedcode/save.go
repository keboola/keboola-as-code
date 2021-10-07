package sharedcode

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type writer struct {
	*files
	fs        filesystem.Fs
	logger    *zap.SugaredLogger
	naming    model.Naming
	state     *model.State
	config    *model.Config
	configRow *model.ConfigRow
	errors    *utils.Error
}

func Save(logger *zap.SugaredLogger, fs filesystem.Fs, naming model.Naming, state *model.State, files *model.ObjectFiles) error {
	configRow := files.Object.(*model.ConfigRow)
	config := state.Get(configRow.ConfigKey()).RemoteOrLocalState().(*model.Config)
	w := &writer{
		fs:        fs,
		files:     files,
		logger:    logger,
		naming:    naming,
		state:     state,
		config:    config,
		configRow: configRow,
		errors:    utils.NewMultiError(),
	}
	return w.save()
}

func (w *writer) save() error {
	// Load content from config row JSON
	rowContent := w.configRow.Content
	normalizeContent(rowContent)

	// Load content
	raw, found := rowContent.Get(model.ShareCodeContentKey)
	if !found {
		return fmt.Errorf(`key "%s" not found in %s`, model.ShareCodeContentKey, w.configRow.Desc())
	}

	// Content must be string
	codeContent, ok := raw.(string)
	if !ok {
		return fmt.Errorf(`key "%s" must be string in %s`, model.ShareCodeContentKey, w.configRow.Desc())
	}

	// Get target component of the shared code -> needed for file extension
	targetComponentId, err := getTargetComponentId(w.config)
	if err != nil {
		return err
	}

	// Remove code content from JSON
	rowContent.Delete(model.ShareCodeContentKey)

	// Generate code file
	codeFilePath := w.naming.SharedCodeFilePath(w.Record.RelativePath(), targetComponentId)
	codeFile := filesystem.CreateFile(codeFilePath, codeContent).SetDescription(`shared code`)
	w.files.Extra = append(w.files.Extra, codeFile)

	// Remove "isDisabled" unnecessary value from "meta.json".
	// Shared code is represented as config row
	// and always contains `"isDisabled": false` in metadata.
	meta := w.files.Metadata
	if meta != nil && meta.Content != nil {
		if value, found := meta.Content.Get(`isDisabled`); found {
			if v, ok := value.(bool); ok && !v {
				// Found `"isDisabled": false` -> delete
				meta.Content.Delete(`isDisabled`)
			}
		}
	}

	return nil
}
