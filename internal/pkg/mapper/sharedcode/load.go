package sharedcode

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type files = model.ObjectFiles

type loader struct {
	*files
	fs        filesystem.Fs
	logger    *zap.SugaredLogger
	naming    model.Naming
	state     *model.State
	config    *model.Config
	configRow *model.ConfigRow
	errors    *utils.Error
}

// Load - load shared code from filesystem to target config.
func Load(logger *zap.SugaredLogger, fs filesystem.Fs, naming model.Naming, state *model.State, files *model.ObjectFiles) error {
	configRow := files.Object.(*model.ConfigRow)
	config := state.Get(configRow.ConfigKey()).LocalState().(*model.Config)
	l := &loader{
		fs:        fs,
		files:     files,
		logger:    logger,
		naming:    naming,
		state:     state,
		config:    config,
		configRow: configRow,
		errors:    utils.NewMultiError(),
	}
	return l.load()
}

func (l *loader) load() error {
	// Get target component of the shared code -> needed for file extension
	targetComponentId, err := getTargetComponentId(l.config)
	if err != nil {
		return err
	}

	// Load file
	codeFilePath := l.naming.SharedCodeFilePath(l.Record.RelativePath(), targetComponentId)
	codeFile, err := l.fs.ReadFile(codeFilePath, `shared code`)
	if err != nil {
		return err
	}
	l.Record.AddRelatedPath(codeFilePath)

	// Set to config row JSON
	l.configRow.Content.Set(CodeContentRowJsonKey, codeFile.Content)
	normalizeContent(l.configRow.Content)
	return nil
}
