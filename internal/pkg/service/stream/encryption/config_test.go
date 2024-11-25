package encryption

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestConfig_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		Name          string
		ExpectedError string
		Config        Config
	}{
		{
			Name:          "empty",
			ExpectedError: `"provider" is a required field`,
			Config:        Config{},
		},
		{
			Name:          "invalid provider",
			ExpectedError: `"provider" must be one of [native gcp aws azure]`,
			Config: Config{
				Provider: "foo",
			},
		},
		{
			Name:          "default  invalid",
			Config:        NewConfig(),
			ExpectedError: "- \"native.secretKey\" is a required field\n- \"gcp.kmsKeyId\" is a required field\n- \"aws.region\" is a required field\n- \"aws.kmsKeyId\" is a required field\n- \"azure.keyVaultUrl\" is a required field\n- \"azure.keyName\" is a required field",
		},
		{
			Name:          "native: nil",
			ExpectedError: "\"native\" is a required field",
			Config: Config{
				Provider: ProviderNative,
			},
		},
		{
			Name:          "native: empty",
			ExpectedError: "\"native.secretKey\" is a required field",
			Config: Config{
				Provider: ProviderNative,
				Native:   &NativeConfig{},
			},
		},
		{
			Name:          "native: length",
			ExpectedError: "\"native.secretKey\" must be 32 characters in length",
			Config: Config{
				Provider: ProviderNative,
				Native: &NativeConfig{
					SecretKey: "0",
				},
			},
		},
		{
			Name: "native: ok",
			Config: Config{
				Provider: ProviderNative,
				Native: &NativeConfig{
					SecretKey: "12345678901234567890123456789012",
				},
			},
		},
		{
			Name:          "gcp: nil",
			ExpectedError: "\"gcp\" is a required field",
			Config: Config{
				Provider: ProviderGCP,
			},
		},
		{
			Name:          "gcp: empty",
			ExpectedError: "\"gcp.kmsKeyId\" is a required field",
			Config: Config{
				Provider: ProviderGCP,
				GCP:      &GCPConfig{},
			},
		},
		{
			Name: "gcp: ok",
			Config: Config{
				Provider: ProviderGCP,
				GCP: &GCPConfig{
					KMSKeyID: "123",
				},
			},
		},
		{
			Name:          "aws: nil",
			ExpectedError: "\"aws\" is a required field",
			Config: Config{
				Provider: ProviderAWS,
			},
		},
		{
			Name:          "aws: empty",
			ExpectedError: "- \"aws.region\" is a required field\n- \"aws.kmsKeyId\" is a required field",
			Config: Config{
				Provider: ProviderAWS,
				AWS:      &AWSConfig{},
			},
		},
		{
			Name: "aws: ok",
			Config: Config{
				Provider: ProviderAWS,
				AWS: &AWSConfig{
					Region:   "eu",
					KMSKeyID: "123",
				},
			},
		},
		{
			Name:          "azure: nil",
			ExpectedError: "\"azure\" is a required field",
			Config: Config{
				Provider: ProviderAzure,
			},
		},
		{
			Name:          "azure: empty",
			ExpectedError: "- \"azure.keyVaultUrl\" is a required field\n- \"azure.keyName\" is a required field",
			Config: Config{
				Provider: ProviderAzure,
				Azure:    &AzureConfig{},
			},
		},
		{
			Name:          "azure: url",
			ExpectedError: "\"azure.keyVaultUrl\" must be a valid URL",
			Config: Config{
				Provider: ProviderAzure,
				Azure: &AzureConfig{
					KeyVaultURL: "vault",
					KeyName:     "123",
				},
			},
		},
		{
			Name: "azure: ok",
			Config: Config{
				Provider: ProviderAzure,
				Azure: &AzureConfig{
					KeyVaultURL: "https://vault",
					KeyName:     "123",
				},
			},
		},
	}

	// Run test cases
	ctx := context.Background()
	val := validator.New()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			err := val.Validate(ctx, tc.Config)
			if tc.ExpectedError == "" {
				assert.NoError(t, err, tc.Name)
			} else if assert.Error(t, err, tc.Name) {
				assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
			}
		})
	}
}
