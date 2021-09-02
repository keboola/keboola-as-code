package options

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/spf13/cast"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"keboola-as-code/src/interaction"
	"keboola-as-code/src/json"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/utils"
)

type parser = viper.Viper

// Options contains parsed flags and ENV variables.
type Options struct {
	*parser
	Verbose           bool   `flag:"verbose"`           // verbose mode, print details to console
	VerboseApi        bool   `flag:"verbose-api"`       // log each API request and response
	LogFilePath       string `flag:"log-file"`          // path to the log file
	ApiHost           string `flag:"storage-api-host"`  // api host
	ApiToken          string `flag:"storage-api-token"` // api token
	workingDirectory  string // working directory
	projectDirectory  string // project directory with ".keboola" metadata dir
	metadataDirectory string // ".keboola" metadata dir
}

func NewOptions() *Options {
	// Env parser
	envNaming := &envNamingConvention{}
	return &Options{parser: viper.NewWithOptions(viper.EnvKeyReplacer(envNaming))}
}

func (o *Options) WorkingDirectory() string {
	if len(o.workingDirectory) == 0 {
		panic(fmt.Errorf("working directory is not set"))
	}
	return o.workingDirectory
}

func (o *Options) ProjectDir() string {
	if len(o.projectDirectory) == 0 {
		panic(fmt.Errorf("project directory is not set"))
	}
	return o.projectDirectory
}

func (o *Options) MetadataDir() string {
	if len(o.metadataDirectory) == 0 {
		panic(fmt.Errorf("metadata directory is not set"))
	}
	return o.metadataDirectory
}

func (o *Options) HasProjectDirectory() bool {
	return len(o.projectDirectory) > 0
}

func (o *Options) SetWorkingDirectory(dir string) error {
	if !utils.IsDir(dir) {
		wd, _ := os.Getwd()
		return fmt.Errorf("working directory \"%s\" not found, pwd:%s", dir, wd)
	}
	o.workingDirectory = utils.AbsPath(dir)
	return nil
}

func (o *Options) SetProjectDirectory(projectDir string) error {
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	if !utils.IsDir(projectDir) {
		return fmt.Errorf("project directory \"%s\" not found", o.projectDirectory)
	}
	if !utils.IsDir(metadataDir) {
		return fmt.Errorf("metadata directory \"%s\" not found", o.metadataDirectory)
	}
	o.projectDirectory = utils.AbsPath(projectDir)
	o.metadataDirectory = utils.AbsPath(metadataDir)
	return nil
}

// BindPersistentFlags for all commands.
func (o *Options) BindPersistentFlags(flags *pflag.FlagSet) {
	flags.SortFlags = true
	flags.BoolP("help", "h", false, "print help for command")
	flags.StringP("log-file", "l", "", "path to a log file for details")
	flags.StringP("working-dir", "d", "", "use other working directory")
	flags.StringP("storage-api-token", "t", "", "storage API token from your project")
	flags.BoolP("verbose", "v", false, "print details")
	flags.BoolP("verbose-api", "", false, "log each API request and response")
}

// Validate required options - defined by field name.
func (o *Options) Validate(required []string) string {
	var errors []string
	envNaming := &envNamingConvention{}
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
		case fieldName == "projectDirectory":
			errors = append(
				errors,
				`- None of this and parent directories is project dir.`,
				`  Project directory must contain the ".keboola" metadata directory.`,
				`  Please change working directory to a project directory or use the "init" command.`,
			)
		case len(flag) > 0:
			errors = append(errors, fmt.Sprintf(
				`- Missing %s. Please use "--%s" flag or ENV variable "%s".`,
				fieldNameHumanReadable,
				flag,
				envNaming.Replace(flag),
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

// Load all sources of Options - flags, envs.
func (o *Options) Load(flags *pflag.FlagSet) (warnings []string, err error) {
	// Bind flags
	if err = o.BindPFlags(flags); err != nil {
		return
	}

	// Bind ENV variables
	o.AutomaticEnv()

	// Set Working directory + load .env file if present
	workingDir, err := getWorkingDirectory(o.parser)
	if err != nil {
		return
	}
	if err = o.SetWorkingDirectory(workingDir); err != nil {
		return
	}
	if err = loadDotEnv(o.workingDirectory); err != nil {
		return
	}

	// Set Project directory + load .env file if present
	projectDir, projectDirWarnings := getProjectDirectory(o.workingDirectory)
	warnings = append(warnings, projectDirWarnings...)
	if len(projectDir) > 0 {
		if err = o.SetProjectDirectory(strings.TrimRight(projectDir, string(os.PathSeparator))); err != nil {
			return nil, err
		}
		if err = loadDotEnv(o.projectDirectory); err != nil {
			return
		}
	}

	// For each Options struct field with "flag" tag -> load value from parser
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

	return
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

// getWorkingDirectory from flag or by default from OS.
func getWorkingDirectory(parser *viper.Viper) (string, error) {
	value := parser.GetString("working-dir")
	if len(value) > 0 {
		return value, nil
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", utils.PrefixError("cannot get current working directory", err)
	}
	return dir, nil
}

// getProjectDirectory finds project directory -> working dir or its parent that contains ".keboola" metadata dir.
func getProjectDirectory(workingDir string) (projectDir string, warnings []string) {
	sep := string(os.PathSeparator)
	projectDir = workingDir

	for {
		metadataDir := filepath.Join(projectDir, ".keboola")
		if stat, err := os.Stat(metadataDir); err == nil {
			if stat.IsDir() {
				return projectDir, warnings
			} else {
				warnings = append(warnings, fmt.Sprintf("Expected dir, but found file at \"%s\"", metadataDir))
			}
		} else if !os.IsNotExist(err) {
			warnings = append(warnings, fmt.Sprintf("Cannot check if path \"%s\" exists: %s", metadataDir, err))
		}

		// Check parent directory
		projectDir = filepath.Dir(projectDir)

		// Is root dir? -> ends with separator, or has no separator -> break
		if strings.HasSuffix(projectDir, sep) || strings.Count(projectDir, sep) == 0 {
			break
		}
	}

	return "", warnings
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
