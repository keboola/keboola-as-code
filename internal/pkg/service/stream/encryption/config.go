package encryption

type Config struct {
	Provider Provider      `json:"provider" configKey:"provider" validate:"required,oneof=native gcp aws azure" configUsage:"Encryption provider."`
	Native   *NativeConfig `json:"native" configKey:"native" validate:"required_if=Provider native"`
	GCP      *GCPConfig    `json:"gcp" configKey:"gcp" validate:"required_if=Provider gcp"`
	AWS      *AWSConfig    `json:"aws" configKey:"aws" validate:"required_if=Provider aws"`
	Azure    *AzureConfig  `json:"azure" configKey:"azure" validate:"required_if=Provider azure"`
}

type NativeConfig struct {
	SecretKey string `json:"secretKey" configKey:"secretKey" sensitive:"true" validate:"required,len=32" configUsage:"Secret key for local encryption. Do not use in production."`
}

type GCPConfig struct {
	KMSKeyID string `json:"kmsKeyId" configKey:"kmsKeyId" validate:"required" configUsage:"Key ID in Google Cloud Key Management Service."`
}

type AWSConfig struct {
	Region   string `json:"region" configKey:"region" validate:"required" configUsage:"AWS Region."`
	KMSKeyID string `json:"kmsKeyId" configKey:"kmsKeyId" validate:"required" configUsage:"Key ID in AWS Key Management Service."`
}

type AzureConfig struct {
	KeyVaultURL string `json:"keyVaultUrl" configKey:"keyVaultUrl" validate:"required,url" configUsage:"Azure Key Vault URL."`
	KeyName     string `json:"keyName" configKey:"keyName" validate:"required" configUsage:"Key name in the vault."`
}

func NewConfig() Config {
	return Config{
		Provider: ProviderNone,
		Native:   &NativeConfig{},
		GCP:      &GCPConfig{},
		AWS:      &AWSConfig{},
		Azure:    &AzureConfig{},
	}
}

func (c *Config) Normalize() {
	if c.Provider != ProviderNative {
		c.Native = nil
	}
	if c.Provider != ProviderGCP {
		c.GCP = nil
	}
	if c.Provider != ProviderAWS {
		c.AWS = nil
	}
	if c.Provider != ProviderAzure {
		c.Azure = nil
	}
}
