package cli

import (
	"bufio"
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/log"
	"keboola-as-code/src/utils"
	"os"
	"path"
	"strings"
	"time"
)

// flags from command line
type flags struct {
	workingDirectory string
	logFilePath      string
	verbose          bool
}

// commander contains dependencies for commands
type commander struct {
	flags            *flags
	initialized      bool               // init method was called
	workingDirectory string             // working directory, can be specified by flag
	logFile          *os.File           // log file instance
	logFilePath      string             // log file path specified by flag, or generated temp file, or empty string if no log file
	logFileClear     bool               // is log file temporary? if yes, it will be removed at the end, if no error occurs
	logger           *zap.SugaredLogger // log to stderr and logFile
}

func NewCommander() *commander {
	return &commander{flags: &flags{}}
}

// Execute command or sub-command
func (c *commander) Execute() {
	defer c.tearDown()
	if err := c.rootCommand().Execute(); err != nil {
		c.init() // init commander, it can be uninitialized, if error occurred before PersistentPreRun call

		// Error is already logged to STDOUT, log to file
		c.logger.Debug("Command exited with error: ", err)
		os.Exit(1)
	}

	panic("error")
}

// tearDown makes clean-up after command execution
func (c *commander) tearDown() {
	if err := recover(); err == nil {
		// No error -> remove log file if temporary
		if c.logFile != nil && c.logFileClear {
			if err = c.logFile.Close(); err != nil {
				panic(fmt.Errorf("cannot close log file \"%s\": %s", c.logFilePath, err))
			}
			if err = os.Remove(c.logFilePath); err != nil {
				panic(fmt.Errorf("cannot remove temp log file \"%s\": %s", c.logFilePath, err))
			}
		}
	} else {
		// Error -> process and close log file
		utils.ProcessPanic(err, c.logger, c.logFilePath)
		if c.logFile != nil {
			if err = c.logFile.Close(); err != nil {
				panic(fmt.Errorf("cannot close log file \"%s\": %s", c.logFilePath, err))
			}
		}
	}
}

// init sets logger and working directory, must be called after flags are parsed
func (c *commander) init() {
	if c.initialized {
		return
	}

	c.setupLogger()
	c.setupWorkingDirectory()
	c.logVersion()
	c.logCommand()
	c.initialized = true
}

// setupLogger according to the flags
func (c *commander) setupLogger() {
	logFile, logFileErr := c.getLogFile()
	c.logger = log.NewLogger(logFile, c.flags.verbose)
	c.logFile = logFile

	// Warn if user specified log file and it cannot be opened
	if logFileErr != nil && !c.logFileClear {
		c.logger.Warnf("Cannot open log file: %s", logFileErr)
	}
}

// setupWorkingDirectory from flag or current working directory
func (c *commander) setupWorkingDirectory() {
	if len(c.flags.workingDirectory) > 0 {
		c.workingDirectory = c.flags.workingDirectory
	} else {
		dir, err := os.Getwd()
		if err != nil {
			panic(fmt.Errorf("cannot get current working directory: %s", err))
		}
		c.workingDirectory = dir
	}
}

func (c *commander) logVersion() {
	versionLines := bufio.NewScanner(strings.NewReader(c.version()))
	for versionLines.Scan() {
		c.logger.Debug(versionLines.Text())
	}
}

func (c *commander) logCommand() {
	c.logger.Debugf("Running command %v", os.Args)
}

// Get log file defined in the flags or create a temp file
func (c *commander) getLogFile() (logFile *os.File, logFileErr error) {
	if len(c.flags.logFilePath) > 0 {
		c.logFilePath = c.flags.logFilePath
		c.logFileClear = false // log file defined by user will be preserved
	} else {
		c.logFilePath = path.Join(os.TempDir(), fmt.Sprintf("keboola-as-code-%d.txt", time.Now().Unix()))
		c.logFileClear = true // temp log file will be removed. It will be preserved only in case of error
	}

	logFile, logFileErr = os.OpenFile(c.logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if logFileErr != nil {
		c.logFilePath = ""
	}
	return
}
