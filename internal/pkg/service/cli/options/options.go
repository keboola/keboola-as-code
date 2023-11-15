package options

import (
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

const (
	NonInteractiveOpt  = `non-interactive`
	StorageAPIHostOpt  = `storage-api-host`
	StorageAPITokenOpt = `storage-api-token`
)

type parser = viper.Viper

const (
	EnvPrefix = "KBC_"
)

// Options manages parsed flags, ENV files and ENV variables.
type Options struct {
	*parser
	envNaming   *env.NamingConvention
	envs        *env.Map
	setBy       map[string]configmap.SetBy
	Verbose     bool   // verbose mode, print details to console
	VerboseAPI  bool   // log each API request and response
	LogFilePath string // path to the log file
	LogFormat   string // stdout and stderr format
}

func New() *Options {
	envNaming := env.NewNamingConvention(EnvPrefix)
	return &Options{
		envNaming: envNaming,
		setBy:     make(map[string]configmap.SetBy),
		parser:    viper.New(),
		LogFormat: "console",
	}
}

func (o *Options) Load(logger log.Logger, osEnvs *env.Map, fs filesystem.Fs, flags *pflag.FlagSet) error {
	// Load ENVs from OS and files
	o.envs = o.loadEnvFiles(logger, osEnvs, fs)

	// Define mapping between flag and field path
	flagToField := func(flag *pflag.Flag) (orderedmap.Path, bool) {
		return orderedmap.PathFromStr(flag.Name), true
	}

	// Bind all flags and corresponding ENVs
	if setBy, err := configmap.BindToViper(o.parser, flags, flagToField, o.envs, o.envNaming, nil); err != nil {
		return err
	} else {
		for k, v := range setBy {
			o.setBy[k] = v
		}
	}

	// Load global options
	o.Verbose = o.GetBool(`verbose`)
	o.VerboseAPI = o.GetBool(`verbose-api`)
	o.LogFilePath = o.GetString(`log-file`)
	o.LogFormat = o.GetString(`log-format`)
	return nil
}

func (o *Options) GetEnvName(flagName string) string {
	return o.envNaming.FlagToEnv(flagName)
}

func (o *Options) Set(key string, value any) {
	o.parser.Set(key, value)
	o.setBy[key] = configmap.SetManually
}

// KeySetBy method informs how the value of the key was set.
func (o *Options) KeySetBy(key string) configmap.SetBy {
	return o.setBy[key]
}

func (o *Options) loadEnvFiles(logger log.Logger, osEnvs *env.Map, fs filesystem.Fs) *env.Map {
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
	return env.LoadDotEnv(logger, osEnvs, fs, dirs)
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
