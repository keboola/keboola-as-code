package k8sapp

import (
	"context"
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Sleep patches spec.state = "Stopped" on the CRD of the workload that owns the
// route, and reports whether it suspended anything.
//
// Production is suspended through the App CR even though the workload is its
// member Sandbox: the operator's product controller reconciles the App's state
// onto the member unconditionally, so a direct member patch is reverted on its
// next pass.
//
// The object is read live rather than from the cache, which can lag a state
// change by longer than it takes to decide to suspend, and the patch carries
// the resourceVersion just read, so a change between the read and the patch is
// rejected by the apiserver.
//
// expectMember closes the gap the resourceVersion cannot: the decision was made
// about one member, and an App redeployed since then is still Running under a
// new one. Patching on the old member's idleness would stop a deployment that
// has just started. Empty skips the check, which is what a Sandbox route wants,
// since its ref already names the workload.
func (w *StateWatcher) Sleep(ctx context.Context, ref WorkloadRef, expectMember string) (bool, error) {
	e, ok := w.entryFor(ref)
	if !ok {
		return false, errors.Errorf("workload %q is not in the cache, nothing was suspended", ref)
	}

	gvr := gvrFor(ref)
	obj, err := w.client.Resource(gvr).Namespace(w.namespace).Get(ctx, e.k8sName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	state, _, err := unstructured.NestedString(obj.Object, "status", "currentState")
	if err != nil {
		return false, err
	}
	if AppActualState(state) != AppActualStateRunning {
		return false, nil
	}

	if !ref.IsSandbox() && expectMember != "" {
		member, _, err := unstructured.NestedString(obj.Object, "status", "productionSandbox")
		if err != nil {
			return false, err
		}
		if member != expectMember {
			return false, nil
		}
	}

	// spec.state only. Writing autoRestartEnabled here would leave the workload
	// in the state the operator treats as "do not start", which the next
	// request could not wake.
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"resourceVersion": obj.GetResourceVersion()},
		"spec":     map[string]any{"state": AppActualStateStopped},
	})
	if err != nil {
		return false, err
	}

	if _, err := w.client.Resource(gvr).Namespace(w.namespace).Patch(
		ctx,
		e.k8sName,
		k8stypes.MergePatchType,
		patch,
		metav1.PatchOptions{},
	); err != nil {
		return false, err
	}

	return true, nil
}
