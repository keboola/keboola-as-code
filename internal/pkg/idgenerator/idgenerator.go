// nolint: gochecknoglobals
package idgenerator

import gonanoid "github.com/matoous/go-nanoid/v2"

const (
	RequestIDLength               = 15
	TemplateInstanceIDLength      = 25
	EtcdNamespaceForE2ETestLength = 20
	ReceiverSecretLength          = 48
)

// alphabet used in ID generation.
var alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RequestID() string {
	return gonanoid.MustGenerate(alphabet, RequestIDLength)
}

func TemplateInstanceID() string {
	return gonanoid.MustGenerate(alphabet, TemplateInstanceIDLength)
}

func EtcdNamespaceForTest() string {
	return gonanoid.MustGenerate(alphabet, EtcdNamespaceForE2ETestLength)
}

func ReceiverSecret() string {
	return gonanoid.MustGenerate(alphabet, ReceiverSecretLength)
}

func Random(length int) string {
	return gonanoid.MustGenerate(alphabet, length)
}
