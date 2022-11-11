// nolint: gochecknoglobals
package idgenerator

import gonanoid "github.com/matoous/go-nanoid/v2"

const (
	RequestIdLength               = 15
	TemplateInstanceIdLength      = 25
	EtcdNamespaceForE2ETestLength = 10
	ReceiverSecretLength          = 32
)

// alphabet used in ID generation.
var alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RequestId() string {
	return gonanoid.MustGenerate(alphabet, RequestIdLength)
}

func TemplateInstanceId() string {
	return gonanoid.MustGenerate(alphabet, TemplateInstanceIdLength)
}

func EtcdNamespaceForTest() string {
	return gonanoid.MustGenerate(alphabet, EtcdNamespaceForE2ETestLength)
}

func ReceiverSecret() string {
	return gonanoid.MustGenerate(alphabet, ReceiverSecretLength)
}
