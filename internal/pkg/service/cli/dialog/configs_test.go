package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestSelectConfigInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, console := createDialogs(t, true)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "3"}, Name: `Config 3`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		require.NoError(t, console.ExpectString("LABEL:"))

		require.NoError(t, console.ExpectString("Config 1 (foo.bar:1)"))

		require.NoError(t, console.ExpectString("Config 2 (foo.bar:2)"))

		require.NoError(t, console.ExpectString("Config 3 (foo.bar:3)"))

		// down arrow -> select Config 2
		require.NoError(t, console.SendDownArrow())
		require.NoError(t, console.SendEnter())

		require.NoError(t, console.ExpectEOF())
	}()

	// Run
	out, err := dialog.SelectConfig(allConfigs, `LABEL`, configmap.NewValue(config2.String()))
	assert.Same(t, config2, out)
	require.NoError(t, err)

	// Close terminal
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())
}

func TestSelectConfigByFlag(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "3"}, Name: `Config 3`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3}

	// Run
	out, err := dialog.SelectConfig(allConfigs, `LABEL`, configmap.Value[string]{Value: "2", SetBy: configmap.SetByFlag})
	assert.Same(t, config2, out)
	require.NoError(t, err)
}

func TestSelectConfigNonInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "3"}, Name: `Config 3`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3}

	// Run
	_, err := dialog.SelectConfig(allConfigs, `LABEL`, configmap.NewValue(""))
	require.ErrorContains(t, err, "please specify config")
}

func TestSelectConfigMissing(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "3"}, Name: `Config 3`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3}

	// Run
	out, err := dialog.SelectConfig(allConfigs, `LABEL`, configmap.Value[string]{Value: "", SetBy: configmap.SetByDefault})
	assert.Nil(t, out)
	require.Error(t, err)
	assert.Equal(t, `please specify config`, err.Error())
}

func TestSelectConfigsInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, console := createDialogs(t, true)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "3"}, Name: `Config 3`}}
	config4 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "4"}, Name: `Config 4`}}
	config5 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "5"}, Name: `Config 5`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3, config4, config5}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		require.NoError(t, console.ExpectString("LABEL:"))

		require.NoError(t, console.ExpectString("Config 1 (foo.bar:1)"))

		require.NoError(t, console.ExpectString("Config 2 (foo.bar:2)"))

		require.NoError(t, console.ExpectString("Config 3 (foo.bar:3)"))

		require.NoError(t, console.ExpectString("Config 4 (foo.bar:4)"))

		require.NoError(t, console.ExpectString("Config 5 (foo.bar:5)"))

		require.NoError(t, console.SendDownArrow()) // -> Config 2

		require.NoError(t, console.SendSpace()) // -> select

		require.NoError(t, console.SendDownArrow()) // -> Config 3

		require.NoError(t, console.SendDownArrow()) // -> Config 4

		require.NoError(t, console.SendSpace()) // -> select

		require.NoError(t, console.SendEnter()) // -> confirm

		require.NoError(t, console.ExpectEOF())
	}()

	// Run
	out, err := dialog.SelectConfigs(allConfigs, `LABEL`, configmap.Value[string]{Value: config2.Name + config4.Name, SetBy: configmap.SetByDefault})
	assert.Equal(t, []*model.ConfigWithRows{config2, config4}, out)
	require.NoError(t, err)

	// Close terminal
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())
}

func TestSelectConfigsByFlag(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "3"}, Name: `Config 3`}}
	config4 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "4"}, Name: `Config 4`}}
	config5 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "5"}, Name: `Config 5`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3, config4, config5}

	// Run
	out, err := dialog.SelectConfigs(allConfigs, `LABEL`, configmap.Value[string]{Value: "foo.bar:2, foo.bar:4", SetBy: configmap.SetByFlag})
	assert.Equal(t, []*model.ConfigWithRows{config2, config4}, out)
	require.NoError(t, err)
}

func TestSelectConfigsMissing(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)

	// All configs
	config1 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "1"}, Name: `Config 1`}}
	config2 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "2"}, Name: `Config 2`}}
	config3 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "3"}, Name: `Config 3`}}
	config4 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "4"}, Name: `Config 4`}}
	config5 := &model.ConfigWithRows{Config: &model.Config{ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: "5"}, Name: `Config 5`}}
	allConfigs := []*model.ConfigWithRows{config1, config2, config3, config4, config5}

	// Run
	out, err := dialog.SelectConfigs(allConfigs, `LABEL`, configmap.NewValue(""))
	assert.Nil(t, out)
	require.Error(t, err)
	assert.Equal(t, `please specify at least one config`, err.Error())
}
