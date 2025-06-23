package validator

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/go-playground/validator/v10"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (v *wrapper) registerCustomMessages() {
	v.registerErrorMessage("required_if", "{0} is a required field")
	v.registerErrorMessage("required_unless", "{0} is a required field")
	v.registerErrorMessage("required_with", "{0} is a required field")
	v.registerErrorMessage("excluded_if", "{0} should not be set")
	v.registerErrorMessage("excluded_unless", "{0} should not be set")
	v.registerErrorMessage("excluded_without", "{0} should not be set")
}

func (v *wrapper) registerCustomRules() {
	v.RegisterRule(
		// Register default validation for "required_in_project"
		// Some values are required in the project scope, but ignored in the template scope.
		// We validate them by default.
		Rule{
			Tag: "required_in_project",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				if v, _ := ctx.Value(DisableRequiredInProjectKey).(bool); v {
					// Template mode, value is valid.
					return true
				}
				// Project mode, value must be set.
				return !fl.Field().IsZero()
			},
			ErrorMsg: "{0} is a required field",
		},
		Rule{
			Tag: "required_not_empty",
			Func: func(fl validator.FieldLevel) bool {
				// Check nil
				if !fl.Field().IsValid() {
					return false
				}
				// Check empty
				return fl.Field().Len() > 0
			},
			ErrorMsg: "{0} is a required field",
		},
		// Alphanumeric string with allowed dash character.
		Rule{
			Tag: "alphanumdash",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				return regexpcache.MustCompile(`^[a-zA-Z0-9\-]+$`).MatchString(fl.Field().String())
			},
			ErrorMsg: "{0} can only contain alphanumeric characters and dash",
		},
		// Template icon
		Rule{
			Tag: "templateicon",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				value := fl.Field().String()
				if after, ok := strings.CutPrefix(value, "component:"); ok {
					value = after
					return len(value) > 0
				}
				if after, ok := strings.CutPrefix(value, "common:"); ok {
					value = after
					return allowedTemplateIcons[value]
				}
				return false
			},
			ErrorMsg: "{0} does not contain an allowed icon",
		},
		Rule{
			Tag: "mdmax",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				value := fl.Field().String()
				paramStr := fl.Param()
				param, err := strconv.Atoi(paramStr)
				if err != nil {
					panic(fmt.Sprintf("failed to convert mdmax param \"%v\" to an int", paramStr))
				}
				value = strhelper.StripMarkdown(value)
				return len(value) <= param
			},
			ErrorMsgFunc: func(fe validator.FieldError) string {
				return fmt.Sprintf("%s exceeded maximum length of %s", fe.Tag(), fe.Param())
			},
		},
		Rule{
			Tag: "minBytes",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				value, ok := fl.Field().Interface().(datasize.ByteSize)
				if !ok {
					panic(errors.Errorf(`unexpected type "%T"`, fl.Field().Interface()))
				}
				param, err := datasize.ParseString(fl.Param())
				if !ok {
					panic(errors.Errorf(`param "%s" is not valid: %w`, fl.Param(), err))
				}
				if param == 0 {
					panic(errors.Errorf(`param "%s" is not valid`, fl.Param()))
				}
				return value >= param
			},
			ErrorMsgFunc: func(fe validator.FieldError) string {
				param, _ := datasize.ParseString(fe.Param())
				return fmt.Sprintf(`must be %s or greater`, param)
			},
		},
		Rule{
			Tag: "maxBytes",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				value, ok := fl.Field().Interface().(datasize.ByteSize)
				if !ok {
					panic(errors.Errorf(`unexpected type "%T"`, fl.Field().Interface()))
				}
				param, err := datasize.ParseString(fl.Param())
				if !ok {
					panic(errors.Errorf(`param "%s" is not valid: %w`, fl.Param(), err))
				}
				if param == 0 {
					panic(errors.Errorf(`param "%s" is not valid`, fl.Param()))
				}
				return value <= param
			},
			ErrorMsgFunc: func(fe validator.FieldError) string {
				param, _ := datasize.ParseString(fe.Param())
				return fmt.Sprintf(`must be %s or less`, param)
			},
		},
		Rule{
			Tag: "minDuration",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				var value time.Duration
				if v, ok := fl.Field().Interface().(time.Duration); ok {
					value = v
				} else if v, ok := fl.Field().Interface().(duration.Duration); ok {
					value = v.Duration()
				} else {
					panic(errors.Errorf(`unexpected type "%T"`, fl.Field().Interface()))
				}

				param, err := time.ParseDuration(fl.Param())
				if err != nil {
					panic(errors.Errorf(`param "%s" is not valid: %w`, fl.Param(), err))
				}

				return value >= param
			},
			ErrorMsgFunc: func(fe validator.FieldError) string {
				param, _ := time.ParseDuration(fe.Param())
				return fmt.Sprintf(`must be %s or greater`, param)
			},
		},
		Rule{
			Tag: "maxDuration",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				var value time.Duration
				if v, ok := fl.Field().Interface().(time.Duration); ok {
					value = v
				} else if v, ok := fl.Field().Interface().(duration.Duration); ok {
					value = v.Duration()
				} else {
					panic(errors.Errorf(`unexpected type "%T"`, fl.Field().Interface()))
				}

				param, err := time.ParseDuration(fl.Param())
				if err != nil {
					panic(errors.Errorf(`param "%s" is not valid: %w`, fl.Param(), err))
				}

				return value <= param
			},
			ErrorMsgFunc: func(fe validator.FieldError) string {
				param, _ := time.ParseDuration(fe.Param())
				return fmt.Sprintf(`must be %s or less`, param.String())
			},
		},
	)
}

func (v *wrapper) registerCustomTypes() {
	// Convert value to string using String method
	v.validator.RegisterCustomTypeFunc(func(field reflect.Value) any {
		if v, ok := field.Interface().(fmt.Stringer); ok {
			return v.String()
		}
		return field
	}, model.SemVersion{})
}
