// nolint: gochecknoglobals
package idgenerator

import gonanoid "github.com/matoous/go-nanoid/v2"

const (
	RequestIdLength               = 15
	TemplateInstanceIdLength      = 25
	EtcdNamespaceForE2ETestLength = 10
)

// alphabet used in ID generation.
var alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RequestId() string {
	return gonanoid.MustGenerate(alphabet, RequestIdLength)
}

func TemplateInstanceId() string {
	return gonanoid.MustGenerate(alphabet, TemplateInstanceIdLength)
}

func EtcdNamespaceForE2ETest() string {
	return gonanoid.MustGenerate(alphabet, EtcdNamespaceForE2ETestLength)
}
