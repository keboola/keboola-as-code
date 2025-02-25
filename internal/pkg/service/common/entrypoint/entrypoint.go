// Package entrypoint provides common code for services entrypoints.
package entrypoint

import (
	"context"
	"fmt"
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Config struct {
	ENVPrefix string
}

// Run methods provides:
//   - Flags generation from the appConfig structure.
//   - Binding of flags, ENVS and configuration files to the appConfig.
//   - Validation and normalization of the appConfig, see configmap.ValueWithValidation and configmap.ValueWithNormalization interfaces.
//   - Writing of the help message to STDOUT.
//   - Dumping of the configuration in YAML or JSON format to STDERR.
func Run[C any](runFn func(ctx context.Context, config C, posArgs []string) error, appConfig C, runConfig Config) {
	if err := runOrErr(runFn, appConfig, runConfig); err != nil {
		errMsg := errors.Format(errors.PrefixError(err, "Error"), errors.FormatAsSentences())
		_, _ = os.Stderr.WriteString(errMsg)
		_, _ = os.Stderr.WriteString("\n")
		os.Exit(1)
	}
}

func runOrErr[C any](runFn func(ctx context.Context, config C, posArgs []string) error, appConfig C, runConfig Config) error {
	// Load ENVs
	envs, err := env.FromOs()
	if err != nil {
		return errors.Errorf("cannot load OS envs: %w", err)
	}

	// Bind flags, ENVs and config files to configuration structure
	var posArgs []string // store remaining positional arguments
	err = configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
		Args:                   os.Args,
		EnvNaming:              env.NewNamingConvention(runConfig.ENVPrefix),
		Envs:                   envs,
		PositionalArgsTarget:   &posArgs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}, &appConfig)

	// Print help
	var helpErr configmap.HelpError
	if errors.As(err, &helpErr) {
		fmt.Println(helpErr.Help)
		os.Exit(1)
	}

	// Dump configuration
	var dumpErr configmap.DumpError
	if errors.As(err, &dumpErr) {
		_, _ = os.Stderr.WriteString("Writing configuration dump.\n\n")
		fmt.Println(string(dumpErr.Dump))
		return dumpErr.ValidationError
	}

	// Handle other errors
	if err != nil {
		return err
	}

	// Run
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(errors.New("entrypoint stopped"))

	return runFn(ctx, appConfig, posArgs)
}
