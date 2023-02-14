package job

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/job/run"
)

func RunCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `run [branch1/]component1/config1 [branch2/]component2/config2 ...`,
		Short: helpmsg.Read(`remote/job/run/short`),
		Long:  helpmsg.Read(`remote/job/run/long`),
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Parse options
			localDeps, err := p.DependenciesForLocalCommand()
			if err != nil {
				return err
			}

			options, err := parseJobRunOptions(localDeps.Options(), args)
			if err != nil {
				return err
			}

			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-job-run")

			return run.Run(d.CommandCtx(), options, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().Bool("async", false, "do not wait for job to finish")
	cmd.Flags().String("timeout", "2m", "how long to wait for job to finish")

	return cmd
}

func parseJobRunOptions(opts *options.Options, args []string) (run.RunOptions, error) {
	o := run.RunOptions{}
	o.Async = opts.GetBool("async")

	timeout, err := time.ParseDuration(opts.GetString("timeout"))
	if err != nil {
		return run.RunOptions{}, err
	}
	o.Timeout = timeout

	jobIndex := map[string]int{}
	invalidArgs := errors.NewMultiError()
	for _, arg := range args {
		// parse [branchID]/componentID/configID

		parts := strings.Split(arg, "/")
		if len(parts) < 2 || len(parts) > 3 {
			invalidArgs.Append(errors.Errorf(`invalid job format "%s"`, arg))
			continue
		}

		var branchID keboola.BranchID
		if len(parts) == 3 {
			value, err := strconv.Atoi(parts[0])
			if err != nil {
				invalidArgs.Append(errors.Errorf(`invalid branch ID "%s" in job "%s"`, parts[0], arg))
				continue
			}
			branchID = keboola.BranchID(value)
		}
		componentID := keboola.ComponentID(parts[len(parts)-2])
		configID := keboola.ConfigID(parts[len(parts)-1])

		index, ok := jobIndex[arg]
		if !ok {
			jobIndex[arg] = 1
			index = 0
		} else {
			jobIndex[arg] += 1
		}
		o.Jobs = append(o.Jobs, run.Job{
			Key:         arg + fmt.Sprintf(" (%d)", index),
			BranchID:    branchID,
			ComponentID: componentID,
			ConfigID:    configID,
		})
	}

	err = invalidArgs.ErrorOrNil()
	if err != nil {
		return run.RunOptions{}, err
	}

	return o, nil
}
