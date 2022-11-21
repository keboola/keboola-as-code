package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) SelectConfig(options *options.Options, all []*model.ConfigWithRows, label string) (result *model.ConfigWithRows, err error) {
	if options.IsSet(`config`) {
		if c, err := search.Config(all, options.GetString(`config`)); err == nil {
			result = c
		} else {
			return nil, err
		}
	} else {
		// Show select prompt
		if index, ok := p.SelectIndex(&prompt.SelectIndex{
			Label:   label,
			Options: configsSelectOpts(all),
		}); ok {
			result = all[index]
		}
	}
	if result == nil {
		return nil, errors.New(`please specify config`)
	}

	return result, nil
}

func (p *Dialogs) SelectConfigs(options *options.Options, all []*model.ConfigWithRows, label string) (results []*model.ConfigWithRows, err error) {
	if options.IsSet(`configs`) {
		// Create configs map
		configByKey := make(map[string]*model.ConfigWithRows)
		for _, config := range all {
			configByKey[fmt.Sprintf(`%s:%s`, config.ComponentId, config.Id)] = config
		}

		// Parse user input
		errs := errors.NewMultiError()
		for _, item := range strings.Split(options.GetString(`configs`), `,`) {
			item = strings.TrimSpace(item)
			if len(item) == 0 {
				continue
			}

			// Check [componentId]:[configId] format
			if len(strings.Split(item, `:`)) != 2 {
				errs.Append(errors.Errorf(`cannot parse "%s", must be in "[componentId]:[configId]" format`, item))
				continue
			}

			// Map to config
			if config, ok := configByKey[item]; ok {
				results = append(results, config)
			} else {
				errs.Append(errors.Errorf(`config "%s" not found`, item))
			}
		}
	} else {
		indexes, ok := p.MultiSelectIndex(&prompt.MultiSelectIndex{
			Label:     label,
			Options:   configsSelectOpts(all),
			Validator: prompt.AtLeastOneRequired,
		})
		if ok {
			for _, index := range indexes {
				results = append(results, all[index])
			}
		}
	}

	if len(results) == 0 {
		return nil, errors.New(`please specify at least one config`)
	}

	return results, nil
}

func configsSelectOpts(all []*model.ConfigWithRows) []string {
	selectOpts := make([]string, 0)
	for _, c := range all {
		selectOpts = append(selectOpts, formatConfig(c))
	}
	return selectOpts
}

func formatConfig(config *model.ConfigWithRows) string {
	return fmt.Sprintf(`%s (%s:%s)`, config.ObjectName(), config.ComponentId, config.ObjectId())
}
