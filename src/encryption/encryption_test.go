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
	assert.False(t, isEncrypted("KBC::ComponentSecure::"))
	assert.False(t, isEncrypted("KBC::ConfigSecure::"))
	assert.False(t, isEncrypted("KBC::Secure::aaaa"))
	assert.False(t, isEncrypted("KBC::KV::aaaa"))
	assert.False(t, isEncrypted("KBC::Encrypted=="))
	assert.False(t, isEncrypted("KBC::ComponentProjectEncrypted=="))
	assert.False(t, isEncrypted("KBC::ComponentProjectEncrypted=="))

	assert.False(t, isEncrypted("fooBarKBC::ProjectSecure::aaaa"))
	assert.False(t, isEncrypted("fooBarKBC::ComponentSecure::aaaa"))
	assert.False(t, isEncrypted("fooBarKBC::ConfigSecure::aaaa"))
	assert.False(t, isEncrypted("fooBarKBC::ComponentProjectEncrypted==aaa"))
	assert.False(t, isEncrypted("fooBarKBC::ConfigSecureKV::aaaa"))
	assert.False(t, isEncrypted("KBC::ProjectSecureComponentSecure::aaaaa"))
	assert.False(t, isEncrypted("KBC::ComponentSecureProjectSecure::aaaaa"))
	assert.False(t, isEncrypted("KBC::ComponentSecureConfigSecure::aaaaa"))
	assert.False(t, isEncrypted("KBC::EncryptedComponentProjectEncrypted=="))
	assert.False(t, isEncrypted("KBC::ProjectSecureComponentSecureConfigSecure::aaaaa"))
	assert.False(t, isEncrypted("KBC::ProjectSecureComponentSecureConfigSecureKV::aaaaa"))

	assert.True(t, isEncrypted("KBC::ProjectSecure::adasdasdasdkjashdkjahsdkjahsdkjasd"))
	assert.True(t, isEncrypted("KBC::ConfigSecure::adasdasdasdkjashdkjahsdkjahsdkjasd"))
	assert.True(t, isEncrypted("KBC::ComponentSecure::adasdasdasdkjashdkjahsdkjahsdkjasd"))
	assert.True(t, isEncrypted("KBC::ProjectSecure::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, isEncrypted("KBC::ComponentSecure::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, isEncrypted("KBC::ConfigSecure::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))

	assert.True(t, isEncrypted("KBC::ProjectSecureKV::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, isEncrypted("KBC::ComponentSecureKV::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, isEncrypted("KBC::ConfigSecureKV::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))

	assert.True(t, isEncrypted("KBC::Encrypted==eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, isEncrypted("KBC::ComponentProjectEncrypted==eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, isEncrypted("KBC::ComponentEncrypted==eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
}
