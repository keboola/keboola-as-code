package source

//
// func (r *Repository) Enable(k key.SourceKey, now time.Time) *op.AtomicOp[definition.Source] {
//	return r.update(k, now, versionDescription, func(source definition.Source) (definition.Source, error) {
//		if !source.IsEnabled() {
//			source.Enable(now)
//		}
//		return source, nil
//	})
//}
//
//// undeleteAllFrom the parent key.
//func (r *Repository) enableAllFrom(parentKey fmt.Stringer, now time.Time, enabledWithParent bool) *op.AtomicOp[[]definition.Source] {
//	var allOld, allCreated []definition.Source
//	atomicOp := op.Atomic(r.client, &allCreated)
//
//	// Get or list
//	switch k := parentKey.(type) {
//	case key.SourceKey:
//		atomicOp.ReadOp(r.GetDeleted(k).WithOnResult(func(entity definition.Source) { allOld = []definition.Source{entity} }))
//	default:
//		atomicOp.ReadOp(r.ListDeleted(parentKey).WithAllTo(&allOld))
//	}
//
//	// Iterate all
//	atomicOp.Write(func(ctx context.Context) op.Op {
//		txn := op.Txn(r.client)
//		for _, old := range allOld {
//			old := old
//
//			if old.DeletedWithParent != undeletedWithParent {
//				continue
//			}
//
//			// Mark undeleted
//			created := deepcopy.Copy(old).(definition.Source)
//			created.Undelete(now)
//
//			// Create a new version record, if the entity has been enabled manually
//			if !enabledWithParent {
//				versionDescription := fmt.Sprintf(`Enabled.`, old.Version.Number)
//				created.IncrementVersion(created, now, versionDescription)
//			}
//
//			// Save
//			txn.Merge(r.save(ctx, now, &old, &created))
//			allCreated = append(allCreated, created)
//		}
//		return txn
//	})
//
//	return atomicOp
//}
