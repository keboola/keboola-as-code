package run

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
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

type Flags struct {
	StorageAPIHost string `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	Async          bool   `configKey:"async" configUsage:"do not wait for job to finish"`
	Timeout        string `configKey:"timeout" configUsage:"how long to wait for job to finish"`
}

func DefaultRunFlags() *Flags {
	return &Flags{
		Timeout: "5m",
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `run [branch1/]component1/config1[@tag] [branch2/]component2/config2[@tag] ...`,
		Short: helpmsg.Read(`remote/job/run/short`),
		Long:  helpmsg.Read(`remote/job/run/long`),
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Parse options
			opts, err := parseJobRunOptions(d.Options(), args)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-job-run")

			return run.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

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

	invalidArgs := errors.NewMultiError()
	for _, arg := range args {
		// parse [branchID]/componentID/configID[@tag]
		var branchID keboola.BranchID
		var componentID keboola.ComponentID
		var configID keboola.ConfigID
		var tag string

		parts := strings.Split(arg, "/")
		if len(parts) < 2 || len(parts) > 3 {
			invalidArgs.Append(errors.Errorf(`invalid job format "%s"`, arg))
			continue
		}

		if len(parts) == 3 {
			value, err := strconv.Atoi(parts[0])
			if err != nil {
				invalidArgs.Append(errors.Errorf(`invalid branch ID "%s" in job "%s"`, parts[0], arg))
				continue
			}
			branchID = keboola.BranchID(value)
		}

		componentID = keboola.ComponentID(parts[len(parts)-2])

		configAndTag := strings.Split(parts[len(parts)-1], "@")
		configID = keboola.ConfigID(configAndTag[0])
		if len(configAndTag) > 1 {
			tag = configAndTag[1]
		}

		o.Jobs = append(o.Jobs, run.NewJob(branchID, componentID, configID, tag))
	}

	err = invalidArgs.ErrorOrNil()
	if err != nil {
		return run.RunOptions{}, err
	}

	return o, nil
}
