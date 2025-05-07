package bucket

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/bucket"
)

func AskCreateBucket(branchKey keboola.BranchKey, d *dialog.Dialogs, f Flags) (bucket.Options, error) {
	opts := bucket.Options{BranchKey: branchKey}

	if f.Stage.IsSet() {
		stage := f.Stage.Value
		if stage != keboola.BucketStageIn && stage != keboola.BucketStageOut {
			return opts, errors.Errorf("invalid stage, must be one of: %s, %s", keboola.BucketStageIn, keboola.BucketStageOut)
		}
		opts.Stage = stage
	} else {
		v, ok := d.Select(&prompt.Select{
			Label:   "Select a stage for the bucket",
			Options: []string{keboola.BucketStageIn, keboola.BucketStageOut},
		})
		if !ok {
			return opts, errors.New("missing bucket stage, please specify it")
		}
		opts.Stage = v
	}

	if f.DisplayName.IsSet() {
		opts.DisplayName = f.DisplayName.Value
	} else {
		displayName, ok := d.Ask(&prompt.Question{
			Label: "Enter a display name for the bucket",
		})
		if ok {
			opts.DisplayName = displayName
		}
	}

	if f.Name.IsSet() {
		opts.Name = f.Name.Value
	} else {
		name, ok := d.Ask(&prompt.Question{
			Label:     "Enter a name for the bucket",
			Validator: prompt.ValueRequired,
			Default:   strhelper.NormalizeName(opts.DisplayName),
		})
		if !ok || len(name) == 0 {
			return opts, errors.New("missing name, please specify it")
		}
		opts.Name = name
	}

	if f.Description.IsSet() {
		opts.Description = f.Description.Value
	} else {
		description, ok := d.Ask(&prompt.Question{
			Label: "Enter a description for the bucket",
		})
		if ok {
			opts.Description = description
		}
	}

	return opts, nil
}
