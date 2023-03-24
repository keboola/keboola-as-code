package dialog

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/bucket"
)

type createBucketDeps interface {
	Options() *options.Options
}

func (p *Dialogs) AskCreateBucket(d createBucketDeps) (bucket.Options, error) {
	opts := bucket.Options{}

	if d.Options().IsSet("stage") {
		stage := d.Options().GetString("stage")
		if stage != keboola.BucketStageIn && stage != keboola.BucketStageOut {
			return opts, errors.Errorf("invalid stage, must be one of: %s, %s", keboola.BucketStageIn, keboola.BucketStageOut)
		}
		opts.Stage = stage
	} else {
		v, ok := p.Select(&prompt.Select{
			Label:   "Select a stage for the bucket",
			Options: []string{keboola.BucketStageIn, keboola.BucketStageOut},
		})
		if !ok {
			return opts, errors.New("missing bucket stage, please specify it")
		}
		opts.Stage = v
	}

	if d.Options().IsSet("display-name") {
		opts.DisplayName = d.Options().GetString("display-name")
	} else {
		displayName, ok := p.Ask(&prompt.Question{
			Label: "Enter a display name for the bucket",
		})
		if ok {
			opts.DisplayName = displayName
		}
	}

	if d.Options().IsSet("name") {
		opts.Name = d.Options().GetString("name")
	} else {
		name, ok := p.Ask(&prompt.Question{
			Label:     "Enter a name for the bucket",
			Validator: prompt.ValueRequired,
			Default:   strhelper.NormalizeName(opts.DisplayName),
		})
		if !ok || len(name) == 0 {
			return opts, errors.New("missing name, please specify it")
		}
		opts.Name = name
	}

	if d.Options().IsSet("description") {
		opts.Description = d.Options().GetString("description")
	} else {
		description, ok := p.Ask(&prompt.Question{
			Label: "Enter a description for the bucket",
		})
		if ok {
			opts.Description = description
		}
	}

	return opts, nil
}
