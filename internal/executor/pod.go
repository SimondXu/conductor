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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// Pod labels. A conversation id need not be a valid label value, so pods
// carry a hash of it, and the id itself as an annotation.
const (
	labelComponent    = "app.kubernetes.io/component"
	labelConversation = "conductor.dev/conversation"
	labelGeneration   = "conductor.dev/generation"
	annotationConvID  = "conductor.dev/conversation-id"
	componentHarness  = "harness"
)

// podReadyTimeout bounds scheduling, image pull and startup of a pod.
const podReadyTimeout = 2 * time.Minute

// Pod runs each conversation's harness in its own pod, in one namespace, with
// plain client-go Create, Watch and Delete (PLAN.md block ③). The controller
// must run in the cluster: it dials the pod's IP.
type Pod struct {
	// Client is core/v1 only: the full clientset doubles the binary.
	Client    corev1client.PodsGetter
	Namespace string
	Image     string
	// Command is the harness container's command; Pod appends
	// --host 0.0.0.0 --port <Port>.
	Command []string
	Port    int32
	// EnvFrom is the name of a Secret whose keys become the container's
	// environment, usable in Command as $(NAME). Empty for none.
	EnvFrom string
	// GracePeriod is the pod's terminationGracePeriodSeconds: how long a
	// deleted harness keeps running, and maybe keeps executing a tool
	// (ARCHITECTURE.md §6.2).
	GracePeriod int64
}

var _ Executor = (*Pod)(nil)

func conversationLabel(conversationID string) string {
	sum := sha256.Sum256([]byte(conversationID))
	return hex.EncodeToString(sum[:])[:40]
}

func podName(conversationID string, gen int64) string {
	return fmt.Sprintf("harness-%s-g%d", conversationLabel(conversationID)[:20], gen)
}

// Acquire removes the conversation's pods of other generations and waits
// until they are gone, or their grace period is over: deleting a pod does
// not stop it at once, and only the ledger fences effects (ARCHITECTURE.md
// §6.2). Then it creates the pod for spec.Generation and waits until it is
// ready.
func (p *Pod) Acquire(ctx context.Context, conversationID string, spec Spec) (Target, error) {
	pods := p.Client.Pods(p.Namespace)
	old, err := pods.List(ctx, metav1.ListOptions{LabelSelector: labelConversation + "=" + conversationLabel(conversationID)})
	if err != nil {
		return Target{}, fmt.Errorf("pod executor: list: %w", err)
	}
	for _, o := range old.Items {
		if err := p.deleteAndWait(ctx, o.Name); err != nil {
			return Target{}, err
		}
	}

	name := podName(conversationID, spec.Generation)
	pod, err := pods.Create(ctx, p.podSpec(name, conversationID, spec.Generation), metav1.CreateOptions{})
	if err != nil {
		return Target{}, fmt.Errorf("pod executor: create %s: %w", name, err)
	}
	ip, err := p.waitReady(ctx, pod)
	if err != nil {
		_ = pods.Delete(context.WithoutCancel(ctx), name, metav1.DeleteOptions{})
		return Target{}, err
	}
	return Target{Addr: net.JoinHostPort(ip, strconv.Itoa(int(p.Port))), Kind: "pod", Generation: spec.Generation}, nil
}

// Release deletes the conversation's pod of generation gen, without waiting.
func (p *Pod) Release(ctx context.Context, conversationID string, gen int64) error {
	err := p.Client.Pods(p.Namespace).Delete(ctx, podName(conversationID, gen), metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("pod executor: delete: %w", err)
	}
	return nil
}

func (p *Pod) podSpec(name, conversationID string, gen int64) *corev1.Pod {
	args := append(append([]string{}, p.Command[1:]...), "--host", "0.0.0.0", "--port", strconv.Itoa(int(p.Port)))
	c := corev1.Container{
		Name:            componentHarness,
		Image:           p.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         p.Command[:1],
		Args:            args,
		Ports:           []corev1.ContainerPort{{Name: "harness", ContainerPort: p.Port}},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: p.Port}},
			PeriodSeconds: 1,
		},
	}
	if p.EnvFrom != "" {
		c.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: p.EnvFrom},
		}}}
	}
	grace := p.GracePeriod
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				labelComponent:    componentHarness,
				labelConversation: conversationLabel(conversationID),
				labelGeneration:   strconv.FormatInt(gen, 10),
			},
			Annotations: map[string]string{annotationConvID: conversationID},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{c},
			// A dead harness is replaced by the reconciler at the next
			// generation, never restarted in place.
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &grace,
		},
	}
}

// waitReady watches the pod until it is ready and returns its IP.
func (p *Pod) waitReady(ctx context.Context, pod *corev1.Pod) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, podReadyTimeout)
	defer cancel()
	w, err := p.Client.Pods(p.Namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fields.OneTermEqualSelector("metadata.name", pod.Name).String(),
		ResourceVersion: pod.ResourceVersion,
	})
	if err != nil {
		return "", fmt.Errorf("pod executor: watch %s: %w", pod.Name, err)
	}
	defer w.Stop()
	for {
		if ip, err := readyIP(pod); ip != "" || err != nil {
			return ip, err
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("pod executor: %s not ready: %w", pod.Name, ctx.Err())
		case ev, ok := <-w.ResultChan():
			if !ok {
				return "", fmt.Errorf("pod executor: watch of %s closed", pod.Name)
			}
			switch ev.Type {
			case watch.Deleted:
				if got, ok := ev.Object.(*corev1.Pod); ok && got.Name == pod.Name {
					return "", fmt.Errorf("pod executor: %s deleted before it was ready", pod.Name)
				}
			case watch.Added, watch.Modified:
				if got, ok := ev.Object.(*corev1.Pod); ok && got.Name == pod.Name {
					pod = got
				}
			}
		}
	}
}

func readyIP(pod *corev1.Pod) (string, error) {
	switch pod.Status.Phase {
	case corev1.PodFailed, corev1.PodSucceeded:
		return "", fmt.Errorf("pod executor: %s exited (%s)", pod.Name, pod.Status.Phase)
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue && pod.Status.PodIP != "" {
			return pod.Status.PodIP, nil
		}
	}
	return "", nil
}

// deleteAndWait deletes a pod and polls until it is gone, for at most its
// grace period and a little more.
func (p *Pod) deleteAndWait(ctx context.Context, name string) error {
	pods := p.Client.Pods(p.Namespace)
	if err := pods.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("pod executor: delete %s: %w", name, err)
	}
	deadline := time.Now().Add(time.Duration(p.GracePeriod)*time.Second + 5*time.Second)
	for time.Now().Before(deadline) {
		if _, err := pods.Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return nil // grace period over: the kubelet has killed it, or will
}
