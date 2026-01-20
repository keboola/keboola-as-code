package op_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type testOp struct {
	Operation op.LowLevelOp
	Error     error
}

type testValue struct {
	Foo string `json:"foo"`
}

type opTestCase[R any] struct {
	Name            string
	ProcessorErrors bool
	PutEtcdKey      bool
	PutEtcdKeyValue string
	ExpectedValue   R
	ExpectedLog     []string
	ExpectedError   string
}

func (v testOp) Op(_ context.Context) (op.LowLevelOp, error) {
	return v.Operation, v.Error
}

func (tc opTestCase[R]) Run(t *testing.T, ctx context.Context, client etcd.KV, log *strings.Builder, expected *any, processorErrors *bool, targetPtr *R, op op.WithResult[R]) {
	t.Helper()
	t.Logf(`test case "%s"`, tc.Name)

	// Prepare etcd database
	if tc.PutEtcdKey {
		_, err := client.Do(ctx, etcd.OpPut("key", tc.PutEtcdKeyValue))
		require.NoError(t, err)
	} else {
		_, err := client.Do(ctx, etcd.OpDelete("key"))
		require.NoError(t, err)
	}

	// Invoke operation
	log.Reset()
	*expected = tc.ExpectedValue
	*processorErrors = tc.ProcessorErrors
	result := op.Do(ctx)

	// Check value and logs
	assert.Equal(t, tc.ExpectedValue, result.Result())
	assert.Equal(t, strings.Join(tc.ExpectedLog, "\n"), strings.TrimSpace(log.String()))

	// Check target pointer
	assert.Equal(t, *targetPtr, tc.ExpectedValue)

	// Check error
	err := result.Err()
	if tc.ExpectedError == "" {
		require.NoError(t, err)
	} else if assert.Error(t, err) {
		assert.Equal(t, tc.ExpectedError, err.Error())
	}
}

// TestOpForType_WithProcessorMethods_ScalarType tests all With* methods with a scalar (string) value.
func TestOpForType_WithProcessorMethods_ScalarType(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	factory := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("key"), nil
	}

	mapper := func(ctx context.Context, raw *op.RawResponse) (result string, err error) {
		if response := raw.Get(); response != nil {
			if len(response.Kvs) == 1 {
				return string(raw.Get().Kvs[0].Value), nil
			}
		}
		return "", errors.New("not found")
	}

	log := &strings.Builder{}
	var target string
	var expected any
	var processorErrors bool

	opWithProcessors := op.NewForType[string](client, factory, mapper).
		WithResultTo(&target).
		WithProcessor(func(ctx context.Context, result *op.Result[string]) {
			log.WriteString("WithProcessor\n")
			require.NotNil(t, ctx)
			require.NotNil(t, result)
			if processorErrors {
				result.AddErr(errors.New("error from WithProcessor"))
			}
		}).
		WithOnResult(func(value string) {
			log.WriteString("WithOnResult\n")
			assert.Equal(t, expected, value)
		}).
		WithEmptyResultAsError(func() error {
			log.WriteString("WithEmptyResultAsError\n")
			if processorErrors {
				return errors.New("error from WithEmptyResultAsError")
			} else {
				return nil
			}
		}).
		WithNotEmptyResultAsError(func() error {
			log.WriteString("WithNotEmptyResultAsError\n")
			if processorErrors {
				return errors.New("error from WithNotEmptyResultAsError")
			} else {
				return nil
			}
		}).
		WithResultValidator(func(value string) error {
			log.WriteString("WithResultValidator\n")
			assert.Equal(t, expected, value)
			if processorErrors {
				return errors.New("error from WithResultValidator")
			} else {
				return nil
			}
		})

	// Define test cases
	cases := []opTestCase[string]{
		{
			Name:            "Key Not Found",
			PutEtcdKey:      false,
			ProcessorErrors: false,
			ExpectedValue:   "",
			ExpectedLog:     []string{"WithProcessor"},
			ExpectedError:   "not found",
		},
		{
			Name:            "Key Not Found + Errors",
			PutEtcdKey:      false,
			ProcessorErrors: true,
			ExpectedValue:   "",
			ExpectedLog:     []string{"WithProcessor"},
			ExpectedError:   "- not found\n- error from WithProcessor",
		},
		{
			Name:            "Key Found + Empty",
			PutEtcdKey:      true,
			PutEtcdKeyValue: "",
			ProcessorErrors: false,
			ExpectedValue:   "",
			ExpectedLog:     []string{"WithProcessor", "WithOnResult", "WithEmptyResultAsError", "WithResultValidator"},
			ExpectedError:   "",
		},
		{
			Name:            "Key Found + Empty + Errors",
			PutEtcdKey:      true,
			PutEtcdKeyValue: "",
			ProcessorErrors: true,
			ExpectedValue:   "",
			ExpectedLog:     []string{"WithProcessor"},
			ExpectedError:   "error from WithProcessor",
		},
		{
			Name:            "Key Found",
			PutEtcdKey:      true,
			PutEtcdKeyValue: "foo",
			ExpectedValue:   "foo",
			ProcessorErrors: false,
			ExpectedLog:     []string{"WithProcessor", "WithOnResult", "WithNotEmptyResultAsError", "WithResultValidator"},
			ExpectedError:   "",
		},
		{
			Name:            "Key Found + Errors",
			PutEtcdKey:      true,
			PutEtcdKeyValue: "foo",
			ProcessorErrors: true,
			ExpectedValue:   "foo",
			ExpectedLog:     []string{"WithProcessor"},
			ExpectedError:   "error from WithProcessor",
		},
	}

	// Run test-cases
	for _, tc := range cases {
		tc.Run(t, ctx, client, log, &expected, &processorErrors, &target, opWithProcessors)
	}
}

// TestOpForType_WithProcessorMethods_Pointer tests all With* methods with a scalar (*testData) value.
func TestOpForType_WithProcessorMethods_Pointer(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	factory := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("key"), nil
	}

	mapper := func(ctx context.Context, raw *op.RawResponse) (result *testValue, err error) {
		response := raw.Get()
		if response == nil || len(response.Kvs) != 1 {
			return nil, errors.New("not found")
		}

		value := &testValue{}
		if err := json.Unmarshal(raw.Get().Kvs[0].Value, value); err != nil {
			return nil, err
		}
		if value.Foo == "" {
			// Return nil without error, if the field is not set
			return nil, nil
		}
		return value, nil
	}

	log := &strings.Builder{}
	var target *testValue
	var expected any
	var processorErrors bool

	opWithProcessors := op.NewForType[*testValue](client, factory, mapper).
		WithResultTo(&target).
		WithProcessor(func(ctx context.Context, result *op.Result[*testValue]) {
			log.WriteString("WithProcessor\n")
			require.NotNil(t, ctx)
			require.NotNil(t, result)
			if processorErrors {
				result.AddErr(errors.New("error from WithProcessor"))
			}
		}).
		WithOnResult(func(value *testValue) {
			log.WriteString("WithOnResult\n")
			assert.Equal(t, expected, value)
		}).
		WithEmptyResultAsError(func() error {
			log.WriteString("WithEmptyResultAsError\n")
			if processorErrors {
				return errors.New("error from WithEmptyResultAsError")
			} else {
				return nil
			}
		}).
		WithResultValidator(func(value *testValue) error {
			log.WriteString("WithResultValidator\n")
			assert.Equal(t, expected, value)
			if processorErrors {
				return errors.New("error from WithResultValidator")
			} else {
				return nil
			}
		})

	// Define test cases
	cases := []opTestCase[*testValue]{
		{
			Name:            "Key Not Found",
			PutEtcdKey:      false,
			ProcessorErrors: false,
			ExpectedValue:   nil,
			ExpectedLog:     []string{"WithProcessor"},
			ExpectedError:   "not found",
		},
		{
			Name:            "Key Not Found + Errors",
			PutEtcdKey:      false,
			ProcessorErrors: true,
			ExpectedValue:   nil,
			ExpectedLog:     []string{"WithProcessor"},
			ExpectedError:   "- not found\n- error from WithProcessor",
		},
		{
			Name:            "Key Found + Empty",
			PutEtcdKey:      true,
			PutEtcdKeyValue: "{}",
			ProcessorErrors: false,
			ExpectedValue:   nil,
			ExpectedLog:     []string{"WithProcessor", "WithOnResult", "WithEmptyResultAsError", "WithResultValidator"},
			ExpectedError:   "",
		},
		{
			Name:            "Key Found + Empty + Errors",
			PutEtcdKey:      true,
			PutEtcdKeyValue: "{}",
			ProcessorErrors: true,
			ExpectedValue:   nil,
			ExpectedLog:     []string{"WithProcessor"},
			ExpectedError:   "error from WithProcessor",
		},
		{
			Name:            "Key Found",
			PutEtcdKey:      true,
			PutEtcdKeyValue: `{"Foo": "bar"}`,
			ExpectedValue:   &testValue{Foo: "bar"},
			ProcessorErrors: false,
			ExpectedLog:     []string{"WithProcessor", "WithOnResult", "WithResultValidator"},
			ExpectedError:   "",
		},
		{
			Name:            "Key Found + Errors",
			PutEtcdKey:      true,
			PutEtcdKeyValue: `{"Foo": "bar"}`,
			ProcessorErrors: true,
			ExpectedValue:   &testValue{Foo: "bar"},
			ExpectedLog:     []string{"WithProcessor"},
			ExpectedError:   "error from WithProcessor",
		},
	}

	// Run test-cases
	for _, tc := range cases {
		tc.Run(t, ctx, client, log, &expected, &processorErrors, &target, opWithProcessors)
	}
}
