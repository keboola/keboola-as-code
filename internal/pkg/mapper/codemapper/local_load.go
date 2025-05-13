package codemapper

import (
	"context"
	"os"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapAfterLocalLoad - loads Python code from code.py file and inserts it into the configuration.
func (m *pythonMapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	// Check if this is a Custom Python configuration
	if ok, err := m.isPythonConfig(recipe.Object); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Create local loader
	l := &localLoader{
		State:           m.state,
		LocalLoadRecipe: recipe,
		logger:          m.logger,
		config:          recipe.Object.(*model.Config),
		errors:          errors.NewMultiError(),
	}

	// Load code
	return l.loadCode(ctx)
}

type localLoader struct {
	*state.State
	*model.LocalLoadRecipe
	logger log.Logger
	config *model.Config
	errors errors.MultiError
}

func (l *localLoader) loadCode(ctx context.Context) error {
	// Path to code.py file
	codePath := filesystem.Join(l.Path(), "code.py")

	// Load the file
	file, err := l.Files.
		Load(codePath).
		AddMetadata(filesystem.ObjectKeyMetadata, l.config.Key()).
		SetDescription("Python code").
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeCode).
		ReadFile(ctx)
	if err != nil {
		// If the file doesn't exist, it's not an error
		if os.IsNotExist(err) {
			l.logger.Debugf(ctx, `Python code file "%s" does not exist`, codePath)
			return nil
		}
		l.errors.Append(err)
		return l.errors.ErrorOrNil()
	}

	// Get file content
	content := file.Content

	// Insert code into configuration
	l.setCodeInConfig(content)

	l.logger.Debugf(ctx, `Loaded Python code from "%s"`, codePath)
	return l.errors.ErrorOrNil()
}

func (l *localLoader) setCodeInConfig(code string) {
	// Get or create parameters in the configuration
	parameters, found := l.config.Content.Get("parameters")
	if !found || parameters == nil {
		parameters = orderedmap.New()
		l.config.Content.Set("parameters", parameters)
	}

	// Check if parameters is an OrderedMap
	paramsMap, ok := parameters.(*orderedmap.OrderedMap)
	if !ok {
		// If not an OrderedMap, create a new one
		paramsMap = orderedmap.New()
		l.config.Content.Set("parameters", paramsMap)
	}

	// Insert code into parameters
	paramsMap.Set("code", code)
}
