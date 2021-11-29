package options

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	StorageApiHostOpt  = `storage-api-host`
	StorageApiTokenOpt = `storage-api-token`
)

type parser = viper.Viper

// Options contains parsed flags and ENV variables.
type Options struct {
	*parser
	envNaming   *env.NamingConvention
	envs        *env.Map
	Verbose     bool   // verbose mode, print details to console
	VerboseApi  bool   // log each API request and response
	LogFilePath string // path to the log file
}

func NewOptions() *Options {
	envNaming := env.NewNamingConvention()
	return &Options{
		envNaming: envNaming,
		parser:    viper.New(),
	}
}

func (o *Options) Load(logger *zap.SugaredLogger, osEnvs *env.Map, fs filesystem.Fs, flags *pflag.FlagSet) error {
	// Load ENVs from OS and files
	envs, err := o.loadEnvFiles(osEnvs, fs)
	if err == nil {
		o.envs = envs
	} else {
		logger.Debug(err.Error())
	}

	// Bind all flags and corresponding ENVs
	if err := o.bindFlagsAndEnvs(flags); err != nil {
		return err
	}

	// Load global options
	o.Verbose = o.GetBool(`verbose`)
	o.VerboseApi = o.GetBool(`verbose-api`)
	o.LogFilePath = o.GetString(`log-file`)
	return nil
}

func (o *Options) GetEnvName(flagName string) string {
	return o.envNaming.Replace(flagName)
}

func (o *Options) bindFlagsAndEnvs(flags *pflag.FlagSet) error {
	if err := o.BindPFlags(flags); err != nil {
		return err
	}

	// For each know flag -> search for ENV
	envs := make(map[string]interface{})
	for _, flagName := range o.AllKeys() {
		envName := o.envNaming.Replace(flagName)
		if v, found := o.envs.Lookup(envName); found {
			envs[flagName] = v
		}
	}

	// Set config, it has < priority as flag.
	// ... so flag value is used if set, otherwise env is used.
	return o.MergeConfigMap(envs)
}

func (o *Options) loadEnvFiles(osEnvs *env.Map, fs filesystem.Fs) (*env.Map, error) {
	// File system basePath = projectDir, so here we are using current/top level dir
	projectDir := `.` // nolint
	workingDir := fs.WorkingDir()

	// Dirs with ENVs files
	dirs := make([]string, 0)
	dirs = append(dirs, workingDir)
	if workingDir != projectDir {
		dirs = append(dirs, projectDir)
	}

	// Load ENVs from files
	if envs, err := env.LoadDotEnv(osEnvs, fs, dirs); err == nil {
		return envs, nil
	} else {
		return nil, utils.PrefixError(fmt.Sprintf(`error loading ENV files: %s`, err.Error()), err)
	}
}

// Dump Options for debugging, hide API token.
func (o *Options) Dump() string {
	var parsedOpts []string
	var defaultOpts []string
	for k, v := range o.AllSettings() {
		if token, ok := v.(string); ok && strings.Contains(k, `token`) {
			if len(token) > 7 {
				v = token[0:7] + `*****`
			} else if len(token) > 0 {
				v = `*****`
			}
		}
		pair := fmt.Sprintf(`  %s = %#v`, k, v)

		if o.IsSet(k) {
			parsedOpts = append(parsedOpts, pair)
		} else {
			defaultOpts = append(defaultOpts, pair)
		}
	}

	sort.Strings(parsedOpts)
	sort.Strings(defaultOpts)

	out := ""
	if len(parsedOpts) > 0 {
		out += fmt.Sprintf("Parsed options:\n%s\n", strings.Join(parsedOpts, "\n"))
	}
	if len(defaultOpts) > 0 {
		out += fmt.Sprintf("Default options:\n%s\n", strings.Join(defaultOpts, "\n"))
	}
	return out
}
