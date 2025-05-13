package codemapper

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapBeforeLocalSave - extracts Python code from configuration and saves it to code.py file.
func (m *pythonMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	// Check if this is a Custom Python configuration
	if ok, err := m.isPythonConfig(recipe.Object); err != nil {
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

	// Save
	return w.save(ctx)
}

type localWriter struct {
	*state.State
	*model.LocalSaveRecipe
	config *model.Config
	errors errors.MultiError
}

func (w *localWriter) save(ctx context.Context) error {
	// Get Python code from configuration
	// We assume that the code is stored in the "code" parameter in the configuration
	content, err := w.getCodeFromConfig()
	if err != nil {
		w.errors.Append(err)
		return w.errors.ErrorOrNil()
	}

	// Always save the file, even if the code is empty
	// Path to code.py file
	codePath := filesystem.Join(w.Path(), "code.py")

	// Create file
	w.Files.
		Add(filesystem.NewRawFile(codePath, content)).
		SetDescription("Python code").
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeCode)

	w.Logger().Debugf(ctx, "Saved Python code to %s", codePath)

	return w.errors.ErrorOrNil()
}

func (w *localWriter) getCodeFromConfig() (string, error) {
	// Get parameters from configuration
	parameters, ok := w.config.Content.Get("parameters")
	if !ok {
		return "", nil // Not an error, just no code
	}

	// Convert to OrderedMap
	paramsMap, ok := parameters.(*orderedmap.OrderedMap)
	if !ok {
		return "", errors.New("parameters is not an OrderedMap")
	}

	// Get code
	codeVal, ok := paramsMap.Get("code")
	if !ok {
		return "", nil // Not an error, just no code
	}

	// Convert to string
	code, ok := codeVal.(string)
	if !ok {
		return "", errors.New("code is not a string")
	}

	return code, nil
}
