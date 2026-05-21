package definition_test

import (
	"strings"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestOTLPSource_Validation(t *testing.T) {
	t.Parallel()

	sourceKey := key.SourceKey{
		BranchKey: key.BranchKey{ProjectID: 123, BranchID: 456},
		SourceID:  "my-source",
	}
	validSecret := strings.Repeat("0", 48)

	cases := testvalidation.TestCases[definition.Source]{
		{
			Name:          "nil OTLP section",
			ExpectedError: `"otlp" is a required field`,
			Value: definition.Source{
				SourceKey:     sourceKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeOTLP,
				Name:          "My Source",
				Description:   "My Description",
			},
		},
		{
			Name:          "empty OTLP section",
			ExpectedError: `"otlp.secret" is a required field`,
			Value: definition.Source{
				SourceKey:     sourceKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeOTLP,
				Name:          "My Source",
				Description:   "My Description",
				OTLP:          &definition.OTLPSource{},
			},
		},
		{
			Name:          "short secret",
			ExpectedError: `"otlp.secret" must be 48 characters in length`,
			Value: definition.Source{
				SourceKey:     sourceKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeOTLP,
				Name:          "My Source",
				Description:   "My Description",
				OTLP: &definition.OTLPSource{
					Secret: "tooshort",
				},
			},
		},
		{
			Name: "ok",
			Value: definition.Source{
				SourceKey:     sourceKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SourceTypeOTLP,
				Name:          "My Source",
				Description:   "My Description",
				OTLP: &definition.OTLPSource{
					Secret: validSecret,
				},
			},
		},
	}

	cases.Run(t)
}
