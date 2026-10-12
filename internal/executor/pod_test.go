// Copyright 2026 Simon Xu
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func newPod() (*Pod, *fake.Clientset) {
	cs := fake.NewClientset()
	return &Pod{Client: cs.CoreV1(), Namespace: "ns", Image: "conductor:dev", Command: []string{"conductor", "harness", "fake"},
		Port: 50053, EnvFrom: "db", GracePeriod: 1}, cs
}

// markReady keeps reporting the pod ready, as the kubelet would, until stop.
func markReady(t *testing.T, cs *fake.Clientset, name string, stop <-chan struct{}) {
	t.Helper()
	pods := cs.CoreV1().Pods("ns")
	for {
		select {
		case <-stop:
			return
		case <-time.After(20 * time.Millisecond):
		}
		pod, err := pods.Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			continue
		}
		pod.Status.PodIP = "10.0.0.7"
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		_, _ = pods.UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
	}
}

// Acquire removes the conversation's other generations, creates its own pod
// with the labels recovery relies on, and returns the pod's address once it
// is ready. Release deletes it.
func TestPod_AcquireReplacesOtherGenerations(t *testing.T) {
	p, cs := newPod()
	ctx := context.Background()
	old := p.podSpec(podName("conv", 1), "conv", 1)
	if _, err := cs.CoreV1().Pods("ns").Create(ctx, old, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go markReady(t, cs, podName("conv", 2), stop)

	target, err := p.Acquire(ctx, "conv", Spec{Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	if target != (Target{Addr: "10.0.0.7:50053", Kind: "pod", Generation: 2}) {
		t.Fatalf("target = %+v", target)
	}
	if _, err := cs.CoreV1().Pods("ns").Get(ctx, old.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("generation 1 pod still exists: %v", err)
	}
	pod, err := cs.CoreV1().Pods("ns").Get(ctx, podName("conv", 2), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pod.Labels[labelGeneration] != "2" || pod.Labels[labelConversation] != conversationLabel("conv") ||
		pod.Annotations[annotationConvID] != "conv" || pod.Spec.RestartPolicy != corev1.RestartPolicyNever ||
		*pod.Spec.TerminationGracePeriodSeconds != 1 || pod.Spec.Containers[0].EnvFrom[0].SecretRef.Name != "db" {
		t.Fatalf("pod = %+v", pod)
	}
	if got := pod.Spec.Containers[0].Args; got[len(got)-1] != "50053" || got[0] != "harness" {
		t.Fatalf("args = %v", got)
	}

	if err := p.Release(ctx, "conv", 1); err != nil {
		t.Fatalf("release of a gone generation: %v", err)
	}
	if err := p.Release(ctx, "conv", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().Pods("ns").Get(ctx, pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("released pod still exists: %v", err)
	}
}

// A harness that exits before it is ready fails the acquisition and is
// cleaned up.
func TestPod_AcquireFailsWhenThePodExits(t *testing.T) {
	p, cs := newPod()
	ctx := context.Background()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		pods := cs.CoreV1().Pods("ns")
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			if pod, err := pods.Get(ctx, podName("conv", 1), metav1.GetOptions{}); err == nil {
				pod.Status.Phase = corev1.PodFailed
				_, _ = pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
			}
		}
	}()
	if _, err := p.Acquire(ctx, "conv", Spec{Generation: 1}); err == nil {
		t.Fatal("acquired a failed pod")
	}
	if _, err := cs.CoreV1().Pods("ns").Get(ctx, podName("conv", 1), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed pod left behind: %v", err)
	}
}

// A configured runtime class reaches the pod; none leaves the cluster default.
func TestPod_RuntimeClass(t *testing.T) {
	p, _ := newPod()
	if got := p.podSpec("n", "conv", 1).Spec.RuntimeClassName; got != nil {
		t.Fatalf("runtimeClassName = %q with none configured, want unset", *got)
	}
	p.RuntimeClass = "gvisor"
	if got := p.podSpec("n", "conv", 1).Spec.RuntimeClassName; got == nil || *got != "gvisor" {
		t.Fatalf("runtimeClassName = %v, want gvisor", got)
	}
}
