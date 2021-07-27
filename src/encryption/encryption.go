package encryption

import (
	"fmt"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
	"regexp"
	"strconv"
	"strings"

	"github.com/iancoleman/orderedmap"
	"go.uber.org/zap"
)

func isKeyToEncrypt(key string) bool {
	return strings.HasPrefix(key, "#")
}

func isValueEncrypted(value string) bool {
	return regexp.MustCompile(`^KBC::ProjectSecure::.+$`).MatchString(value)
}

type pathIterator interface {
	String() string
}
type OrderedMapIterator string
type ArrayMapIterator int

func (v OrderedMapIterator) String() string {
	return string(v)
}

func (v ArrayMapIterator) String() string {
	return fmt.Sprintf("[%v]", int(v))
}

type EncryptionValue struct {
	object state.ObjectState // config or configRow state object
	key    string            // key e.g. "#myEncryptedValue"
	value  string            // value to encrypt
	path   []pathIterator    // path to value from config
}

type finder struct {
	errors *utils.Error
	values []EncryptionValue
}

func (f *finder) parseArray(object state.ObjectState, array []interface{}, path []pathIterator) {
	for idx, value := range array {
		f.parseConfigValue(object, strconv.Itoa(idx), value, append(path, ArrayMapIterator(idx)))
	}

}

func (f *finder) parseOrderedMap(object state.ObjectState, content *orderedmap.OrderedMap, path []pathIterator) {
	for _, key := range content.Keys() {
		mapValue, _ := content.Get(key)
		f.parseConfigValue(object, key, mapValue, append(path, OrderedMapIterator(key)))
	}
}

func (f *finder) parseConfigValue(object state.ObjectState, key string, configValue interface{}, path []pathIterator) {
	switch value := configValue.(type) {
	case orderedmap.OrderedMap:
		f.parseOrderedMap(object, &value, path)
	case string:
		if isKeyToEncrypt(key) && !isValueEncrypted(value) {
			f.values = append(f.values, EncryptionValue{object, key, value, path})
		}
	case []interface{}:
		f.parseArray(object, value, path)
	}
}

func (f *finder) FindValues(projectState *state.State) {
	for _, object := range projectState.All() {
		if !object.HasLocalState() {
			continue
		}
		switch o := object.(type) {
		case *state.ConfigState:
			f.parseOrderedMap(o, o.Local.Content, nil)
		case *state.ConfigRowState:
			f.parseOrderedMap(o, o.Local.Content, nil)
		}
	}
}
func FindValuesToEncrypt(projectState *state.State) ([]EncryptionValue, error) {
	f := &finder{utils.NewMultiError(), nil}
	f.FindValues(projectState)
	return f.values, f.errors.ErrorOrNil()
}

func pathToString(path []pathIterator) string {
	result := ""
	isFirst := true
	for _, pathParts := range path {
		if isFirst {
			isFirst = false
			result = pathParts.String()
		} else {
			result = result + "." + pathParts.String()
		}
	}
	return result
}

func PrintValuesToEncrypt(values []EncryptionValue, logger *zap.SugaredLogger) {
	if len(values) == 0 {
		logger.Info("No values to encrypt.")
	} else {
		logger.Info("Values to encrypt:")
		previousObjectId := ""
		for _, value := range values {
			if previousObjectId != value.object.ObjectId() {
				logger.Infof("%v %v", value.object.Kind().Abbr, value.object.RelativePath())
				previousObjectId = value.object.ObjectId()
			}

			logger.Infof("  %v", pathToString(value.path))

		}

	}
}
