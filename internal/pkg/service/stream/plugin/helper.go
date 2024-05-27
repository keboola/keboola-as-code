package plugin

import (
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func isCreatedNow(original any) bool {
	return reflect.ValueOf(original).IsNil()
}

func isDeletedNow(original, updated any) bool {
	o, ok1 := original.(definition.SoftDeletableInterface)
	u, ok2 := updated.(definition.SoftDeletableInterface)
	return !reflect.ValueOf(original).IsNil() && ok1 && ok2 && !o.IsDeleted() && u.IsDeleted()
}

func isUndeletedNow(original, updated any) bool {
	o, ok1 := original.(definition.SoftDeletableInterface)
	u, ok2 := updated.(definition.SoftDeletableInterface)
	return !reflect.ValueOf(original).IsNil() && ok1 && ok2 && o.IsDeleted() && !u.IsDeleted()
}

func isEnabledNow(original, updated any) bool {
	o, ok1 := original.(definition.SwitchableInterface)
	u, ok2 := updated.(definition.SwitchableInterface)
	return !reflect.ValueOf(original).IsNil() && ok1 && ok2 && o.IsDisabled() && u.IsEnabled()
}

func isDisabledNow(original, updated any) bool {
	o, ok1 := original.(definition.SwitchableInterface)
	u, ok2 := updated.(definition.SwitchableInterface)
	return !reflect.ValueOf(original).IsNil() && ok1 && ok2 && o.IsEnabled() && u.IsDisabled()
}

func isActivatedNow(original, updated any) bool {
	return isCreatedNow(original) || isUndeletedNow(original, updated) || isEnabledNow(original, updated)
}

func isDeactivatedNow(original, updated any) bool {
	return isDeletedNow(original, updated) || isDisabledNow(original, updated)
}
