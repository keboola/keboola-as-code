package fixtures

//func CreateSharedCode(t *testing.T, state model.Objects) (model.ConfigKey, []model.ConfigRowKey) {
//	t.Helper()
//
//	// Branch
//	branchKey := model.BranchKey{Id: 123}
//	branch := &model.Branch{BranchKey: branchKey}
//	state.MustAdd(branch)
//
//	// Shared code
//	sharedCodeKey := model.ConfigKey{
//		BranchId:    123,
//		ComponentId: model.SharedCodeComponentId,
//		Id:          `456`,
//	}
//	sharedCode := &model.Config{
//		ConfigKey: sharedCodeKey,
//		Content:   orderedmap.New(),
//		SharedCode: &model.SharedCodeConfig{
//			Target: `keboola.python-transformation-v2`,
//		},
//	}
//	state.MustAdd(sharedCode)
//
//	// Shared code row 1
//	row1Key := model.ConfigRowKey{
//		BranchId:    123,
//		ComponentId: model.SharedCodeComponentId,
//		ConfigId:    `456`,
//		Id:          `1234`,
//	}
//	row1 := &model.ConfigRow{ConfigRowKey: row1Key, Name: "Code 1", Content: orderedmap.New()}
//	state.MustAdd(row1)
//
//	// Shared code row 2
//	row2Key := model.ConfigRowKey{
//		BranchId:    123,
//		ComponentId: model.SharedCodeComponentId,
//		ConfigId:    `456`,
//		Id:          `5678`,
//	}
//	row2 := &model.ConfigRow{ConfigRowKey: row2Key, Name: "Code 2", Content: orderedmap.New()}
//	state.MustAdd(row2)
//	return sharedCodeKey, []model.ConfigRowKey{row1Key, row2Key}
//}
