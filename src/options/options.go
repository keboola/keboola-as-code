package options

import (
	"fmt"
	"github.com/iancoleman/strcase"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
)

// Options contains parsed flags and ENV variables
type Options struct {
	Verbose          bool   `flag:"verbose"`           // verbose mode, print details to console
	LogFilePath      string `flag:"log-file"`          // path to the log file
	ApiUrl           string `flag:"storage-api-url"`   // api url
	ApiToken         string `flag:"storage-api-token"` // api token
	WorkingDirectory string // working directory
	ProjectDirectory string // project directory with ".keboola" metadata dir
}

// BindPersistentFlags for all commands
func (o *Options) BindPersistentFlags(flags *pflag.FlagSet) {
	flags.SortFlags = true
	flags.BoolP("help", "h", false, "print help for command")
	flags.StringP("log-file", "l", "", "path to a log file for details")
	flags.StringP("working-dir", "d", "", "use other working directory")
	flags.StringP("storage-api-url", "u", "", "storage API url, eg. \"connection.keboola.com\"")
	flags.StringP("storage-api-token", "t", "", "storage API token")
	flags.BoolP("verbose", "v", false, "print details")
}

// Validate required options - defined by field name
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
		if fieldName == "ProjectDirectory" {
			errors = append(
				errors,
				`- This or any parent directory is not a Keboola project dir.`,
				`  Project directory must contain ".keboola" metadata directory.`,
				`  Please change working directory to a project directory or create a new with "init" command.`,
			)
		} else if len(flag) > 0 {
			errors = append(errors, fmt.Sprintf(
				`- Missing %s. Please use "--%s" flag or ENV variable "%s".`,
				fieldNameHumanReadable,
				flag,
				envNaming.Replace(flag),
			))
		} else {
			errors = append(errors, fmt.Sprintf(`- Missing %s.`, fieldNameHumanReadable))
		}
	}

	return strings.Join(errors, "\n")
}

// Load all sources of Options - flags, envs
func (o *Options) Load(flags *pflag.FlagSet) (warnings []string, err error) {
	// Env parser
	envNaming := &envNamingConvention{}
	parser := viper.NewWithOptions(viper.EnvKeyReplacer(envNaming))

	// Bind flags
	if err = parser.BindPFlags(flags); err != nil {
		return
	}

	// Bind ENV variables
	parser.AutomaticEnv()

	// Set Working directory + load .env file if present
	o.WorkingDirectory, err = getWorkingDirectory(parser)
	o.WorkingDirectory = strings.TrimRight(o.WorkingDirectory, string(os.PathSeparator))
	if err != nil {
		return
	}
	if err = loadDotEnv(o.WorkingDirectory); err != nil {
		return
	}

	// Set Project directory + load .env file if present
	var projectDirWarnings []string
	o.ProjectDirectory, projectDirWarnings = getProjectDirectory(o.WorkingDirectory)
	o.ProjectDirectory = strings.TrimRight(o.ProjectDirectory, string(os.PathSeparator))
	warnings = append(warnings, projectDirWarnings...)
	if err = loadDotEnv(o.ProjectDirectory); err != nil {
		return
	}

	// For each Options struct field with "flag" tag -> load value from parser
	reflection := reflect.Indirect(reflect.ValueOf(o))
	types := reflect.TypeOf(*o)
	for i := 0; i < reflection.NumField(); i++ {
		if flag := types.Field(i).Tag.Get("flag"); len(flag) > 0 {
			if value := parser.Get(flag); value != nil {
				reflection.Field(i).Set(reflect.ValueOf(value))
			}
		}
	}

	// Normalize the values into a uniform form
	o.normalize()

	return
}

func (o *Options) normalize() {
	o.ApiUrl = strings.TrimRight(o.ApiUrl, "/")
	o.ApiUrl = strings.TrimPrefix(o.ApiUrl, "https://")
	o.ApiToken = strings.TrimSpace(o.ApiToken)
}

// Dump Options for debugging, hide API token
func (o *Options) Dump() string {
	re := regexp.MustCompile(`(ApiToken:"[^"]{1,7})[^"]*(")`)
	str := fmt.Sprintf("Parsed options: %#v", o)
	str = re.ReplaceAllString(str, `$1*****$2`)
	return str
}

// getWorkingDirectory from flag or by default from OS
func getWorkingDirectory(parser *viper.Viper) (string, error) {
	value := parser.GetString("working-dir")
	if len(value) > 0 {
		return value, nil
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("cannot get current working directory: %s", err)
	}
	return dir, nil
}

// getProjectDirectory finds project directory -> working dir or its parent that contains ".keboola" metadata dir
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
