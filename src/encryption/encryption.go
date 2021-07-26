package encryption

import (
	"fmt"
	"keboola-as-code/src/state"
	"strconv"
	"strings"

	"github.com/iancoleman/orderedmap"
	"go.uber.org/zap"
)

func isKeyToEncrypt(property string) bool {
	return strings.HasPrefix(property, "#")
}

func isValueEncrypted(value string) bool {
	return strings.HasPrefix(value, "KBC::ProjectSecure::")
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

type ConfigEncryptionValue struct {
	key   string
	value string
	path  []pathIterator
}

func parseArray(array []interface{}, path []pathIterator) []*ConfigEncryptionValue {
	result := make([]*ConfigEncryptionValue, 0)
	for idx, value := range array {
		result = append(result, parseConfigValue(strconv.Itoa(idx), value, append(path, ArrayMapIterator(idx)))...)
	}
	return result
}

func parseOrderedMap(config *orderedmap.OrderedMap, path []pathIterator) []*ConfigEncryptionValue {
	result := make([]*ConfigEncryptionValue, 0)
	for _, key := range config.Keys() {
		mapValue, _ := config.Get(key)
		result = append(result, parseConfigValue(key, mapValue, append(path, OrderedMapIterator(key)))...)
	}
	return result
}

func parseConfigValue(key string, configValue interface{}, path []pathIterator) []*ConfigEncryptionValue {
	result := make([]*ConfigEncryptionValue, 0)
	switch value := configValue.(type) {
	case orderedmap.OrderedMap:
		result = append(result, parseOrderedMap(&value, path)...)
	case string:
		if isKeyToEncrypt(key) && !isValueEncrypted(value) {
			result = append(result, &ConfigEncryptionValue{key, value, path})
		}
	case []interface{}:
		result = append(result, parseArray(value, path)...)
	}
	return result
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

func getOrderedMapEncryptionInfo(orderedMap *orderedmap.OrderedMap, object state.ObjectState, logger *zap.SugaredLogger) string {
	result := ""
	encryptionValues := parseOrderedMap(orderedMap, make([]pathIterator, 0))
	if len(encryptionValues) > 0 {
		result = fmt.Sprintf("%v %v\n", object.Kind().Abbr, object.RelativePath())
		for _, value := range encryptionValues {
			result += fmt.Sprintf("  %v\n", pathToString(value.path))
		}
	}
	return result
}

func FindValues(projectState *state.State, logger *zap.SugaredLogger) {
	configs := projectState.Configs()
	configRows := projectState.ConfigRows()

	info := ""
	for _, config := range configs {
		info += getOrderedMapEncryptionInfo(config.Local.Content, config, logger)
	}
	for _, configRow := range configRows {
		info += getOrderedMapEncryptionInfo(configRow.Local.Content, configRow, logger)
	}
	if info == "" {
		logger.Info("No values to encrypt.")
	} else {
		logger.Info("Values to encrypt:")
		logger.Info(info)
	}

}
