package k8sapp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func decodeApp(t *testing.T, spec, status map[string]any) appObject {
	t.Helper()

	u := map[string]any{
		"apiVersion": Group + "/" + Version,
		"kind":       "App",
		"metadata":   map[string]any{"name": "app-1", "namespace": "keboola"},
		"spec":       spec,
		"status":     status,
	}

	var obj appObject
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(u, &obj))
	return obj
}

// The product-role marker is the ABSENCE of spec.containerSpec, so appSpec.ContainerSpec
// must decode to nil for an absent field and to non-nil for a present one — including a
// present-but-empty object. The rest of appSpec uses value types, which cannot express
// that distinction; this test is what stops the field being "simplified" back into one.
//
// It asserts the decoded shape rather than the resulting token, so a change in how
// FromUnstructured maps this field fails here, naming the decode, instead of surfacing
// only as a token that is silently no longer read.
func TestAppObject_ContainerSpecDecodesAbsentAsNil(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		spec          map[string]any
		wantNil       bool
		wantIsProduct bool
	}{
		"absent": {
			spec:          map[string]any{"appId": "1"},
			wantNil:       true,
			wantIsProduct: true,
		},
		"present with content": {
			spec:          map[string]any{"appId": "1", "containerSpec": map[string]any{"image": "keboola/data-app:latest"}},
			wantNil:       false,
			wantIsProduct: false,
		},
		"present but empty": {
			spec:          map[string]any{"appId": "1", "containerSpec": map[string]any{}},
			wantNil:       false,
			wantIsProduct: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			obj := decodeApp(t, tc.spec, map[string]any{})

			if tc.wantNil {
				assert.Nil(t, obj.Spec.ContainerSpec,
					"spec.containerSpec is absent and must decode to a nil map — the product-role marker is absence, "+
						"and a type that cannot represent absence makes isProduct() always false")
			} else {
				assert.NotNil(t, obj.Spec.ContainerSpec,
					"spec.containerSpec is present and must decode to a non-nil map — decoding it as nil makes every "+
						"workload App read as product-role")
			}

			assert.Equal(t, tc.wantIsProduct, obj.isProduct())
		})
	}
}

// The gate keys on role, not on spec.runtime.backend.type alone: a product App's
// spec.runtime describes no live workload, while a workload App's is authoritative.
func TestAppObject_E2BAccessTokenSecretName(t *testing.T) {
	t.Parallel()

	const secret = "e2b-access-token"

	runtimeSpec := func(backendType string) map[string]any {
		return map[string]any{"backend": map[string]any{"type": backendType}}
	}
	containerSpec := map[string]any{"image": "keboola/data-app:latest"}

	tests := map[string]struct {
		spec   map[string]any
		status map[string]any
		want   string
	}{
		"product app without runtime": {
			spec:   map[string]any{"appId": "1"},
			status: map[string]any{"e2bSandbox": map[string]any{"accessTokenSecretName": secret}},
			want:   secret,
		},
		"product app with k8s runtime residue from adoption": {
			spec:   map[string]any{"appId": "1", "runtime": runtimeSpec("k8sDeployment")},
			status: map[string]any{"e2bSandbox": map[string]any{"accessTokenSecretName": secret}},
			want:   secret,
		},
		"e2b workload app": {
			spec:   map[string]any{"appId": "1", "containerSpec": containerSpec, "runtime": runtimeSpec(BackendTypeE2BSandbox)},
			status: map[string]any{"e2bSandbox": map[string]any{"accessTokenSecretName": secret}},
			want:   secret,
		},
		"k8s workload app with stale status after a backend switch": {
			spec:   map[string]any{"appId": "1", "containerSpec": containerSpec, "runtime": runtimeSpec("k8sDeployment")},
			status: map[string]any{"e2bSandbox": map[string]any{"accessTokenSecretName": secret}},
			want:   "",
		},
		"no secret name": {
			spec:   map[string]any{"appId": "1"},
			status: map[string]any{},
			want:   "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			obj := decodeApp(t, tc.spec, tc.status)
			assert.Equal(t, tc.want, obj.e2bAccessTokenSecretName())
		})
	}
}
