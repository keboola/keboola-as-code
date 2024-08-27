// nolint: gochecknoglobals
package idgenerator

import gonanoid "github.com/matoous/go-nanoid/v2"

const (
	RequestIDLength               = 15
	TemplateInstanceIDLength      = 25
	EtcdNamespaceForE2ETestLength = 20
	ReceiverSecretLength          = 48
	TaskExceptionIDLength         = 15
)

// alphabet used in ID generation.
var alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func Random(length int) string {
	// nolint: forbidigo
	return gonanoid.MustGenerate(alphabet, length)
}

func RequestID() string {
	return Random(RequestIDLength)
}

func TemplateInstanceID() string {
	return Random(TemplateInstanceIDLength)
}

func EtcdNamespaceForTest() string {
	return Random(EtcdNamespaceForE2ETestLength)
}

func StreamHTTPSourceSecret() string {
	return Random(ReceiverSecretLength)
}

func TaskExceptionID() string {
	return Random(TaskExceptionIDLength)
}
