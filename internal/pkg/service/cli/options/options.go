package options

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const (
	StorageAPIHostOpt  = `storage-api-host`
	StorageAPITokenOpt = `storage-api-token`
)

type parser = viper.Viper

type SetBy int

const (
	SetByUnknown SetBy = iota
	SetByFlag
	SetByFlagDefault
	SetByEnv
	SetManually
)

// Options manages parsed flags, ENV files and ENV variables.
type Options struct {
	*parser
	envNaming   *env.NamingConvention
	envs        *env.Map
	setBy       map[string]SetBy
	Verbose     bool   // verbose mode, print details to console
	VerboseAPI  bool   // log each API request and response
	LogFilePath string // path to the log file
}

func New() *Options {
	envNaming := env.NewNamingConvention()
	return &Options{
		envNaming: envNaming,
		setBy:     make(map[string]SetBy),
		parser:    viper.New(),
	}
}

func (o *Options) Load(logger log.Logger, osEnvs *env.Map, fs filesystem.Fs, flags *pflag.FlagSet) error {
	// Load ENVs from OS and files
	o.envs = o.loadEnvFiles(logger, osEnvs, fs)

	// Bind all flags and corresponding ENVs
	if err := o.bindFlagsAndEnvs(flags); err != nil {
		return err
	}

	// Load global options
	o.Verbose = o.GetBool(`verbose`)
	o.VerboseAPI = o.GetBool(`verbose-api`)
	o.LogFilePath = o.GetString(`log-file`)
	return nil
}

func (o *Options) GetEnvName(flagName string) string {
	return o.envNaming.Replace(flagName)
}

func (o *Options) Set(key string, value any) {
	o.parser.Set(key, value)
	o.setBy[key] = SetManually
}

// KeySetBy method informs how the value of the key was set.
func (o *Options) KeySetBy(key string) SetBy {
	return o.setBy[key]
}

func (o *Options) bindFlagsAndEnvs(flags *pflag.FlagSet) error {
	if err := o.BindPFlags(flags); err != nil {
		return err
	}

	// For each know flag -> search for ENV
	envs := make(map[string]interface{})
	flags.VisitAll(func(flag *pflag.Flag) {
		envName := o.envNaming.Replace(flag.Name)
		if flag.Changed {
			o.setBy[flag.Name] = SetByFlag
		} else if v, found := o.envs.Lookup(envName); found {
			envs[flag.Name] = v
			o.setBy[flag.Name] = SetByEnv
		} else {
			o.setBy[flag.Name] = SetByFlagDefault
		}
	})

	// Set config, it has < priority as flag.
	// ... so flag value is used if set, otherwise ENV is used.
	return o.MergeConfigMap(envs)
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
