package encryption

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsKeyToEncrypt(t *testing.T) {
	assert.True(t, isKeyToEncrypt("#keyToEncrypt"))
	assert.True(t, isKeyToEncrypt("##aa"))
	assert.True(t, isKeyToEncrypt("#vaule"))

	assert.False(t, isKeyToEncrypt("k#eyToEncrypt"))
	assert.False(t, isKeyToEncrypt("aa"))
	assert.False(t, isKeyToEncrypt("aabc#"))
}

func TestIsValueEncrypted(t *testing.T) {
	assert.False(t, isEncrypted("somevalue"))
	assert.False(t, isEncrypted("kbc:value"))
	assert.False(t, isEncrypted("kbc::project"))
	assert.False(t, isEncrypted("KBC::ProjectSecure::"))
	assert.False(t, isEncrypted("fooBarKBC::ProjectSecure::aaaa"))

	assert.True(t, isEncrypted("KBC::ProjectSecure::adasdasdasdkjashdkjahsdkjahsdkjasd"))
	assert.True(t, isEncrypted("KBC::ProjectSecure::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))

}
