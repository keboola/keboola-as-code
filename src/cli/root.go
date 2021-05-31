package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"keboola-as-code/src/log"
	"os"
	"path"
	"time"
)

var desc =
`Keboola Connection 
pull/push client for components configurations.

Configurations can be synchronized in both
directions [KBC project] <-> [a local directory].
`

// rootCommand, parent of all sub-commands
func (c *commander) rootCommand() *cobra.Command{
	// Define command
	cmdName := path.Base(os.Args[0])
	cmd := &cobra.Command{
		Use: cmdName,
		Version: c.version(),
		Short: desc,
	}
	cmd.SetVersionTemplate("{{.Version}}")

	// Flags
	cmd.PersistentFlags().SortFlags = true
	cmd.PersistentFlags().BoolP("help", "h", false, "print help for command")
	cmd.PersistentFlags().StringVarP(&c.flags.workingDirectory, "dir", "d", "", "use other working directory")
	cmd.PersistentFlags().StringVarP(&c.flags.logFilePath, "log-file", "l", "", "path to a log file for details")
	cmd.PersistentFlags().BoolVarP(&c.flags.verbose, "verbose", "v", false, "print details")

	// Logger
	c.setupLogger()

	// Sub-commands
	cmd.AddCommand(
		c.initCommand(),
	)

	return cmd
}

// setupLogger log file and verbosity according to the flags
func (c *commander) setupLogger() {
	logFile, logFileErr := c.getLogFile()
	c.logger = log.NewLogger(logFile, c.flags.verbose)
	if logFileErr != nil {
		c.logger.Warnf("Cannot open log file: %s", logFileErr)
	}
}

// Get log file defined in the flags or create a temp file
func (c *commander) getLogFile() (file *os.File, err error) {
	if len(c.flags.logFilePath) == 0 {
		c.flags.logFilePath = path.Join(os.TempDir(), fmt.Sprintf("keboola-as-code-%d.txt", time.Now().Unix()))
	}

	return os.OpenFile(c.flags.logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
}
