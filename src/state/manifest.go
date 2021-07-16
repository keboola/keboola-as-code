package state

import (
	"fmt"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
)

func (b *BranchState) UpdateManifest(m *manifest.Manifest, rename bool) {
	var branch *model.Branch
	if b.Local != nil {
		branch = b.Local
	} else if b.Remote != nil {
		branch = b.Remote
	} else {
		panic(fmt.Errorf("branch Local or Remote state must be set"))
	}

	if b.Path == "" || rename {
		if branch.IsDefault {
			b.Path = "main"
		} else {
			b.Path = m.Naming.BranchPath(branch)
		}
	}

	b.ResolveParentPath()
}

func (c *ConfigState) UpdateManifest(m *manifest.Manifest, rename bool) {
	var config *model.Config
	if c.Local != nil {
		config = c.Local
	} else if c.Remote != nil {
		config = c.Remote
	} else {
		panic(fmt.Errorf("config Local or Remote state must be set"))
	}

	// Get branch manifest
	branchKey := c.BranchKey()
	branchManifest, found := m.GetRecord(branchKey)
	if !found {
		panic(fmt.Errorf("branch manifest wit key \"%s\" not found", branchKey))
	}

	// Set paths
	if c.Path == "" || rename {
		c.Path = m.Naming.ConfigPath(c.Component, config)
	}
	c.ResolveParentPath(branchManifest.(*manifest.BranchManifest))
}

func (r *ConfigRowState) UpdateManifest(m *manifest.Manifest, rename bool) {
	var row *model.ConfigRow
	if r.Local != nil {
		row = r.Local
	} else if r.Remote != nil {
		row = r.Remote
	} else {
		panic(fmt.Errorf("config Local or Remote state must be set"))
	}

	// Get config
	configKey := row.ConfigKey()
	configManifest, found := m.GetRecord(configKey)
	if !found {
		panic(fmt.Errorf("config manifest wit key \"%s\" not found", configKey))
	}

	// Set paths
	if r.Path == "" || rename {
		r.Path = m.Naming.ConfigRowPath(row)
	}
	r.ResolveParentPath(configManifest.(*manifest.ConfigManifest))
}
