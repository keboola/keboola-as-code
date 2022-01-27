package encryptionapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsKeyToEncrypt(t *testing.T) {
	t.Parallel()
	assert.True(t, IsKeyToEncrypt("#keyToEncrypt"))
	assert.True(t, IsKeyToEncrypt("##aa"))
	assert.True(t, IsKeyToEncrypt("#vaule"))

	assert.False(t, IsKeyToEncrypt("k#eyToEncrypt"))
	assert.False(t, IsKeyToEncrypt("aa"))
	assert.False(t, IsKeyToEncrypt("aabc#"))
}

func TestIsValueEncrypted(t *testing.T) {
	t.Parallel()
	assert.False(t, IsEncrypted("somevalue"))
	assert.False(t, IsEncrypted("kbc:value"))
	assert.False(t, IsEncrypted("kbc::project"))
	assert.False(t, IsEncrypted("KBC::ProjectSecure::"))
	assert.False(t, IsEncrypted("KBC::ComponentSecure::"))
	assert.False(t, IsEncrypted("KBC::ConfigSecure::"))
	assert.False(t, IsEncrypted("KBC::Secure::aaaa"))
	assert.False(t, IsEncrypted("KBC::KV::aaaa"))
	assert.False(t, IsEncrypted("KBC::Encrypted=="))
	assert.False(t, IsEncrypted("KBC::ComponentProjectEncrypted=="))
	assert.False(t, IsEncrypted("KBC::ComponentProjectEncrypted=="))

	assert.False(t, IsEncrypted("fooBarKBC::ProjectSecure::aaaa"))
	assert.False(t, IsEncrypted("fooBarKBC::ComponentSecure::aaaa"))
	assert.False(t, IsEncrypted("fooBarKBC::ConfigSecure::aaaa"))
	assert.False(t, IsEncrypted("fooBarKBC::ComponentProjectEncrypted==aaa"))
	assert.False(t, IsEncrypted("fooBarKBC::ConfigSecureKV::aaaa"))
	assert.False(t, IsEncrypted("KBC::ProjectSecureComponentSecure::aaaaa"))
	assert.False(t, IsEncrypted("KBC::ComponentSecureProjectSecure::aaaaa"))
	assert.False(t, IsEncrypted("KBC::ComponentSecureConfigSecure::aaaaa"))
	assert.False(t, IsEncrypted("KBC::EncryptedComponentProjectEncrypted=="))
	assert.False(t, IsEncrypted("KBC::ProjectSecureComponentSecureConfigSecure::aaaaa"))
	assert.False(t, IsEncrypted("KBC::ProjectSecureComponentSecureConfigSecureKV::aaaaa"))

	assert.True(t, IsEncrypted("KBC::ProjectSecure::adasdasdasdkjashdkjahsdkjahsdkjasd"))
	assert.True(t, IsEncrypted("KBC::ConfigSecure::adasdasdasdkjashdkjahsdkjahsdkjasd"))
	assert.True(t, IsEncrypted("KBC::ComponentSecure::adasdasdasdkjashdkjahsdkjahsdkjasd"))
	assert.True(t, IsEncrypted("KBC::ProjectSecure::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, IsEncrypted("KBC::ComponentSecure::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, IsEncrypted("KBC::ConfigSecure::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))

	assert.True(t, IsEncrypted("KBC::ProjectSecureKV::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, IsEncrypted("KBC::ComponentSecureKV::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, IsEncrypted("KBC::ConfigSecureKV::eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))

	assert.True(t, IsEncrypted("KBC::Encrypted==eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, IsEncrypted("KBC::ComponentProjectEncrypted==eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
	assert.True(t, IsEncrypted("KBC::ComponentEncrypted==eJwBLAHT/mE6Mjp7aTowO3M6ODU6It71AgCKBbvO16JAsWfGBSx39OlMWMfPEAQdPT1tkQVGyZx4XlUbanQNKOeaWG3mwIlhOr17Ugd1mhNW/7riCnjZYh9PElRGNT8lGCCsd/2GKUxt55ciO2k6MTtzOjE4NDoiAQIDAHhlXs9v5x5d+klIkL9bzyaH5qzvWHJt2fGW9czDhWdtwAFI+Wj+aR1kRMcVpSENnQrTAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMAqc7gfKtoV/LmHqTAgEQgDuT3jNjIsuo0pWeqYEFTb+9WjLdQOwryRl9OFVUmLyCfcSS1i+ej2JgdAWWcK4YihI6hPr0WHauvucRmCI7fZQxd4E="))
}
