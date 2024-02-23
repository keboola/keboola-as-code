package dialog

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) AskBucketID(all []*keboola.Bucket) (keboola.BucketID, error) {
	if p.options.IsSet(`bucket`) {
		return keboola.ParseBucketID(p.options.GetString(`bucket`))
	}

	selectOpts := make([]string, 0)
	for _, b := range all {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, b.DisplayName, b.BucketID.String()))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   "Select a bucket",
		Options: selectOpts,
	}); ok {
		return all[index].BucketID, nil
	}

	return keboola.BucketID{}, errors.New(`please specify bucket`)
}
