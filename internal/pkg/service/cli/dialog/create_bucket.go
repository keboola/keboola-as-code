package dialog

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/bucket"
)

func (p *Dialogs) AskCreateBucket(branchKey keboola.BranchKey) (bucket.Options, error) {
	opts := bucket.Options{BranchKey: branchKey}

	if p.options.IsSet("stage") {
		stage := p.options.GetString("stage")
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

	if p.options.IsSet("display-name") {
		opts.DisplayName = p.options.GetString("display-name")
	} else {
		displayName, ok := p.Ask(&prompt.Question{
			Label: "Enter a display name for the bucket",
		})
		if ok {
			opts.DisplayName = displayName
		}
	}

	if p.options.IsSet("name") {
		opts.Name = p.options.GetString("name")
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

	if p.options.IsSet("description") {
		opts.Description = p.options.GetString("description")
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
