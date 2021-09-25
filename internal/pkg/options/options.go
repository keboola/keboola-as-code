package options

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/joho/godotenv"
	"github.com/spf13/cast"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/interaction"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type parser = viper.Viper

// Options contains parsed flags and ENV variables.
type Options struct {
	*parser
	envNaming   *env.NamingConvention
	Verbose     bool   `flag:"verbose"`           // verbose mode, print details to console
	VerboseApi  bool   `flag:"verbose-api"`       // log each API request and response
	LogFilePath string `flag:"log-file"`          // path to the log file
	ApiHost     string `flag:"storage-api-host"`  // api host
	ApiToken    string `flag:"storage-api-token"` // api token
}

func NewOptions() *Options {
	envNaming := env.NewNamingConvention()
	return &Options{
		envNaming: envNaming,
		parser: viper.NewWithOptions(
			viper.EnvKeyReplacer(envNaming),
		),
	}
}

func (o *Options) Load(logger *zap.SugaredLogger, fs filesystem.Fs, flags *pflag.FlagSet) error {
	// Bind flags
	if err := o.BindPFlags(flags); err != nil {
		return err
	}

	// Automatic fallback to ENVs, see env.NamingConvention
	o.AutomaticEnv()

	// Load ENVs from OS and files
	envs, err := o.loadEnvFiles(fs)
	if err != nil {
		logger.Debug(err.Error())
	}

	// Set loaded ENVs
	for k, v := range envs {
		if strings.HasPrefix(k, env.Prefix) {
			logger.Debugf(`Found ENV "%s"`, k)
			utils.MustSetEnv(k, v)
		}
	}

	// Map values to Options struct
	reflection := reflect.Indirect(reflect.ValueOf(o))
	types := reflect.TypeOf(*o)
	for i := 0; i < reflection.NumField(); i++ {
		field := types.Field(i)
		if flag := field.Tag.Get("flag"); len(flag) > 0 {
			value := castValue(o.Get(flag), field.Type.Kind())
			if value != nil {
				reflection.Field(i).Set(reflect.ValueOf(value))
			}
		}
	}

	// Normalize the values into a uniform form
	o.normalize()

	return nil
}

func (o *Options) loadEnvFiles(fs filesystem.Fs) (map[string]string, error) {
	// File system basePath = projectDir, so here we are using current/top level dir
	projectDir := `.` // nolint
	workingDir := fs.WorkingDir()

	// Load ENVs from OS
	osEnvs, err := godotenv.Parse(strings.NewReader(strings.Join(os.Environ(), "\n")))
	if err != nil {
		return nil, err
	}

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

// Validate required options - defined by field name.
func (o *Options) Validate(required []string) string {
	var errors []string
	reflection := reflect.Indirect(reflect.ValueOf(o))
	types := reflect.TypeOf(*o)

	// Iterate over required fields
	for _, fieldName := range required {
		fieldType, exists := types.FieldByName(fieldName)
		fieldNameHumanReadable := strcase.ToDelimited(fieldName, ' ')
		if !exists {
			panic(fmt.Sprintf("Filed \"%s\" doesn't exist in Options struct.", fieldName))
		}

		flag := fieldType.Tag.Get("flag")
		if reflection.FieldByName(fieldName).Len() > 0 {
			continue
		}

		// Create error message by field type
		switch {
		case len(flag) > 0:
			errors = append(errors, fmt.Sprintf(
				`- Missing %s. Please use "--%s" flag or ENV variable "%s".`,
				fieldNameHumanReadable,
				flag,
				o.envNaming.Replace(flag),
			))
		default:
			errors = append(errors, fmt.Sprintf(`- Missing %s.`, fieldNameHumanReadable))
		}
	}

	return strings.Join(errors, "\n")
}

// AskUser for value if used interactive terminal.
func (o *Options) AskUser(p *interaction.Prompt, fieldName string) {
	switch fieldName {
	case "Host":
		if len(o.ApiHost) == 0 {
			o.ApiHost, _ = p.Ask(&interaction.Question{
				Label:       "API host",
				Description: "Please enter Keboola Storage API host, eg. \"connection.keboola.com\".",
				Validator:   interaction.ApiHostValidator,
			})
		}
	case "ApiToken":
		if len(o.ApiToken) == 0 {
			o.ApiToken, _ = p.Ask(&interaction.Question{
				Label:       "API token",
				Description: "Please enter Keboola Storage API token. The value will be hidden.",
				Hidden:      true,
				Validator:   interaction.ValueRequired,
			})
		}
	default:
		panic(fmt.Sprintf("unexpected field name \"%s\"", fieldName))
	}
}

func (o *Options) normalize() {
	o.ApiHost = strings.TrimRight(o.ApiHost, "/")
	o.ApiHost = strings.TrimPrefix(o.ApiHost, "https://")
	o.ApiHost = strings.TrimPrefix(o.ApiHost, "http://")
	o.ApiToken = strings.TrimSpace(o.ApiToken)
}

// Dump Options for debugging, hide API token.
func (o *Options) Dump() string {
	re := regexp.MustCompile(`("ApiToken":"[^"]{1,7})[^"]*(")`)
	str := fmt.Sprintf("Parsed options: %s", json.MustEncode(o, false))
	str = re.ReplaceAllString(str, `$1*****$2`)
	return str
}

func castValue(val interface{}, kind reflect.Kind) interface{} {
	switch kind {
	case reflect.Bool:
		return cast.ToBool(val)
	case reflect.String:
		return cast.ToString(val)
	case reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
		return cast.ToInt(val)
	case reflect.Uint:
		return cast.ToUint(val)
	case reflect.Uint32:
		return cast.ToUint32(val)
	case reflect.Uint64:
		return cast.ToUint64(val)
	case reflect.Int64:
		return cast.ToInt64(val)
	case reflect.Float64, reflect.Float32:
		return cast.ToFloat64(val)
	default:
		return val
	}
}
