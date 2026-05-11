package definition_test

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func TestSource_FormatOTLPSourceURL(t *testing.T) {
	t.Parallel()

	src := &definition.Source{
		SourceKey: key.SourceKey{
			BranchKey: key.BranchKey{ProjectID: keboola.ProjectID(12345), BranchID: keboola.BranchID(1)},
			SourceID:  "my-source",
		},
		HTTP: &definition.HTTPSource{Secret: "abc123secret"},
	}

	url, err := src.FormatOTLPSourceURL("https://stream.keboola.com")
	require.NoError(t, err)
	assert.Equal(t, "https://stream.keboola.com/otlp/12345/my-source/abc123secret", url)
}

func TestSource_FormatOTLPSourceURL_TrailingSlashInPublicURL(t *testing.T) {
	t.Parallel()

	src := &definition.Source{
		SourceKey: key.SourceKey{
			BranchKey: key.BranchKey{ProjectID: keboola.ProjectID(12345), BranchID: keboola.BranchID(1)},
			SourceID:  "my-source",
		},
		HTTP: &definition.HTTPSource{Secret: "s3cret"},
	}

	// JoinPath should normalize the slash regardless of the input form.
	url, err := src.FormatOTLPSourceURL("https://stream.keboola.com/")
	require.NoError(t, err)
	assert.Equal(t, "https://stream.keboola.com/otlp/12345/my-source/s3cret", url)
}

func TestSource_FormatOTLPSourceURL_InvalidPublicURL(t *testing.T) {
	t.Parallel()

	src := &definition.Source{HTTP: &definition.HTTPSource{Secret: "x"}}
	_, err := src.FormatOTLPSourceURL("\x7f://not a url")
	require.Error(t, err)
}
