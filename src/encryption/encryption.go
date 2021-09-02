package encryption

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/iancoleman/orderedmap"
	"go.uber.org/zap"

	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

func isKeyToEncrypt(key string) bool {
	return strings.HasPrefix(key, "#")
}

func isEncrypted(value string) bool {
	currentFormatMatch := regexp.MustCompile(`^KBC::(ProjectSecure|ComponentSecure|ConfigSecure)(KV)?::.+$`).MatchString(value)
	legacyFormatMatch := regexp.MustCompile(`^KBC::(Encrypted==|ComponentProjectEncrypted==|ComponentEncrypted==).+$`).MatchString(value)
	return currentFormatMatch || legacyFormatMatch
}

type Group struct {
	object model.ObjectState // config or configRow state object
	values []Value
}

type Value struct {
	key   string        // key e.g. "#myEncryptedValue"
	value string        // value to encrypt
	path  utils.KeyPath // path to value from config
}

type finder struct {
	groups []Group
}

func (g *Group) parseArray(array []interface{}, keyPath utils.KeyPath) {
	for idx, value := range array {
		g.parseConfigValue(strconv.Itoa(idx), value, append(keyPath, utils.SliceStep(idx)))
	}
}

func (g *Group) parseOrderedMap(content *orderedmap.OrderedMap, path utils.KeyPath) {
	for _, key := range content.Keys() {
		mapValue, _ := content.Get(key)
		g.parseConfigValue(key, mapValue, append(path, utils.MapStep(key)))
	}
}

func (g *Group) parseConfigValue(key string, configValue interface{}, path utils.KeyPath) {
	switch value := configValue.(type) {
	case orderedmap.OrderedMap:
		g.parseOrderedMap(&value, path)
	case string:
		if isKeyToEncrypt(key) && !isEncrypted(value) {
			g.values = append(g.values, Value{key, value, path})
		}
	case []interface{}:
		g.parseArray(value, path)
	}
}

func (f *finder) FindValues(projectState *state.State) {
	for _, object := range projectState.All() {
		if !object.HasLocalState() {
			continue
		}

		// Walk through configuration nested structure
		group := Group{object, nil}
		switch o := object.(type) {
		case *model.ConfigState:
			group.parseOrderedMap(o.Local.Content, nil)

		case *model.ConfigRowState:
			group.parseOrderedMap(o.Local.Content, nil)
		}

		// Store group if some values found
		if len(group.values) > 0 {
			f.groups = append(f.groups, group)
		}
	}
}
func FindUnencrypted(projectState *state.State) []Group {
	f := &finder{nil}
	f.FindValues(projectState)
	return f.groups
}

func LogGroups(groups []Group, logger *zap.SugaredLogger) {
	if len(groups) == 0 {
		logger.Info("No values to encrypt.")
		return
	}
	logger.Info("Values to encrypt:")

	for _, group := range groups {
		logger.Infof("%v %v", group.object.Kind().Abbr, group.object.RelativePath())
		for _, value := range group.values {
			logger.Infof("  %v", value.path)
		}
	}
}

func (v *Value) encryptedPath() string {
	return "#" + v.path.String()
}

func prepareMapToEncrypt(values []Value) map[string]string {
	result := make(map[string]string)
	for _, value := range values {
		result[value.encryptedPath()] = value.value
	}
	return result
}

func DoEncrypt(projectState *state.State, unencryptedGroups []Group, api *Api) error {
	errors := utils.NewMultiError()
	localManager := projectState.LocalManager()
	projectId := fmt.Sprintf("%v", projectState.Manifest().Project.Id)
	for _, group := range unencryptedGroups {
		// create map with {"#<value-path>":"<value-to-encrypt>"...} entries
		mapToEncrypt := prepareMapToEncrypt(group.values)

		// type switch on config or configRow state
		switch o := group.object.(type) {
		case *model.ConfigState:
			encryptedMap, encryptionError := api.EncryptMapValues(o.ComponentId, projectId, mapToEncrypt)
			if encryptionError != nil {
				errors.Append(encryptionError)
				continue
			}
			// update local state with encrypted values
			for _, value := range group.values {
				encryptedValue := encryptedMap[value.encryptedPath()]
				o.Local.Content = utils.UpdateIn(o.Local.Content, value.path, encryptedValue)
			}
		case *model.ConfigRowState:
			encryptedMap, encryptionError := api.EncryptMapValues(o.ComponentId, projectId, mapToEncrypt)
			if encryptionError != nil {
				errors.Append(encryptionError)
				continue
			}
			// update local state with encrypted values
			for _, value := range group.values {
				encryptedValue := encryptedMap[value.encryptedPath()]
				o.Local.Content = utils.UpdateIn(o.Local.Content, value.path, encryptedValue)
			}
		}
		// save updated config local state to disk
		if err := localManager.SaveModel(group.object.Manifest(), group.object.LocalState()); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

func ValidateAllEncrypted(projectState *state.State) error {
	unencryptedGroups := FindUnencrypted(projectState)
	errors := utils.NewMultiError()
	for _, group := range unencryptedGroups {
		object := group.object
		valuesErrors := utils.NewMultiError()
		for _, value := range group.values {
			valuesErrors.AppendRaw(value.path.String())
		}
		objectPath := projectState.Naming().ConfigFilePath(object.RelativePath())
		errors.AppendWithPrefix(fmt.Sprintf("%s \"%s\" contains unencrypted values", object.Kind().Name, objectPath), valuesErrors)
	}
	return errors.ErrorOrNil()
}
