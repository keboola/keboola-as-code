package run

import (
	"strconv"
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/job/run"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Async           configmap.Value[bool]   `configKey:"async" configUsage:"do not wait for job to finish"`
	Timeout         configmap.Value[string] `configKey:"timeout" configUsage:"how long to wait for job to finish"`
}

func DefaultFlags() Flags {
	return Flags{
		Timeout: configmap.NewValue("5m"),
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `run [branch1/]component1/config1[@tag] [branch2/]component2/config2[@tag] ...`,
		Short: helpmsg.Read(`remote/job/run/short`),
		Long:  helpmsg.Read(`remote/job/run/long`),
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken, dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Parse options
			opts, err := parseJobRunOptions(args, f)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "remote-job-run")

			return run.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}

func parseJobRunOptions(args []string, f Flags) (run.RunOptions, error) {
	o := run.RunOptions{}
	o.Async = f.Async.Value

	timeout, err := time.ParseDuration(f.Timeout.Value)
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
