package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestSelectConfigInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, console := createDialogs(t, true)
	o := options.New()

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "3"}, Name: `Config 3`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("LABEL:")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 1 (foo.bar:1)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 2 (foo.bar:2)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 3 (foo.bar:3)")
		assert.NoError(t, err)

		_, err = console.SendLine(testhelper.DownArrow) // down arrow -> select Config 2
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	out, err := dialog.SelectConfig(o, allConfigs, `LABEL`)
	assert.Same(t, config2, out)
	assert.NoError(t, err)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestSelectConfigByFlag(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)
	o := options.New()
	o.Set(`config`, `2`)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "3"}, Name: `Config 3`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3}

	// Run
	out, err := dialog.SelectConfig(o, allConfigs, `LABEL`)
	assert.Same(t, config2, out)
	assert.NoError(t, err)
}

func TestSelectConfigMissing(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)
	o := options.New()

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "3"}, Name: `Config 3`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3}

	// Run
	out, err := dialog.SelectConfig(o, allConfigs, `LABEL`)
	assert.Nil(t, out)
	assert.Error(t, err)
	assert.Equal(t, `please specify config`, err.Error())
}

func TestSelectConfigsInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, console := createDialogs(t, true)
	o := options.New()

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "3"}, Name: `Config 3`}}
	config4 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "4"}, Name: `Config 4`}}
	config5 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "5"}, Name: `Config 5`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3, config4, config5}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("LABEL:")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 1 (foo.bar:1)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 2 (foo.bar:2)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 3 (foo.bar:3)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 4 (foo.bar:4)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 5 (foo.bar:5)")
		assert.NoError(t, err)

		_, err = console.Send(testhelper.DownArrow) // -> Config 2
		assert.NoError(t, err)

		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		_, err = console.Send(testhelper.DownArrow) // -> Config 3
		assert.NoError(t, err)

		_, err = console.Send(testhelper.DownArrow) // -> Config 4
		assert.NoError(t, err)

		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		_, err = console.Send(testhelper.Enter) // -> confirm
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	out, err := dialog.SelectConfigs(o, allConfigs, `LABEL`)
	assert.Equal(t, []*model.ConfigWithRows{config2, config4}, out)
	assert.NoError(t, err)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestSelectConfigsByFlag(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)
	o := options.New()
	o.Set(`configs`, `foo.bar:2, foo.bar:4`)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "3"}, Name: `Config 3`}}
	config4 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "4"}, Name: `Config 4`}}
	config5 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "5"}, Name: `Config 5`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3, config4, config5}

	// Run
	out, err := dialog.SelectConfigs(o, allConfigs, `LABEL`)
	assert.Equal(t, []*model.ConfigWithRows{config2, config4}, out)
	assert.NoError(t, err)
}

func TestSelectConfigsMissing(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)
	o := options.New()

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "3"}, Name: `Config 3`}}
	config4 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "4"}, Name: `Config 4`}}
	config5 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: "5"}, Name: `Config 5`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3, config4, config5}

	// Run
	out, err := dialog.SelectConfigs(o, allConfigs, `LABEL`)
	assert.Nil(t, out)
	assert.Error(t, err)
	assert.Equal(t, `please specify at least one config`, err.Error())
}
