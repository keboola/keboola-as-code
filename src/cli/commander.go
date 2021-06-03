package cli

import (
	"go.uber.org/zap"
	"os"
)

type flags struct {
	workingDirectory string
	logFilePath string
	verbose bool
}

type commander struct {
	flags *flags
	logger *zap.SugaredLogger
}

func NewCommander() *commander {
	return &commander{&flags{},  nil}
}

func (c *commander) Execute() {
	if err := c.rootCommand().Execute(); err != nil {
		// Error is already logged to STDOUT, log to file
		c.logger.Debug("Command exited with error: ", err)
		os.Exit(1)
	}
}
