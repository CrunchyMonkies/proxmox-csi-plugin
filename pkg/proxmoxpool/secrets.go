/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxmoxpool

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	defaultTokenIDKey     = "token_id"
	defaultTokenSecretKey = "token_secret"
)

// ResolveTokenRefs resolves any TokenRef set on the given clusters into their
// TokenID/TokenSecret fields by fetching the referenced Kubernetes Secret.
// defaultNamespace is used for any TokenRef that does not specify its own
// Namespace. Clusters without a TokenRef are left untouched. Resolution
// happens once, in-memory; nothing is written back to the Secret or to disk.
func ResolveTokenRefs(ctx context.Context, kclient kubernetes.Interface, defaultNamespace string, clusters []*ProxmoxCluster) error {
	for _, cluster := range clusters {
		ref := cluster.TokenRef
		if ref == nil {
			continue
		}

		namespace := ref.Namespace
		if namespace == "" {
			namespace = defaultNamespace
		}

		if namespace == "" {
			return fmt.Errorf("token_ref for cluster %q: namespace is required (set token_ref.namespace explicitly)", cluster.Region)
		}

		tokenIDKey := ref.TokenIDKey
		if tokenIDKey == "" {
			tokenIDKey = defaultTokenIDKey
		}

		tokenSecretKey := ref.TokenSecretKey
		if tokenSecretKey == "" {
			tokenSecretKey = defaultTokenSecretKey
		}

		secret, err := kclient.CoreV1().Secrets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("token_ref for cluster %q: failed to get secret %s/%s: %w", cluster.Region, namespace, ref.Name, err)
		}

		tokenID, ok := secret.Data[tokenIDKey]
		if !ok {
			return fmt.Errorf("token_ref for cluster %q: secret %s/%s has no key %q", cluster.Region, namespace, ref.Name, tokenIDKey)
		}

		tokenSecret, ok := secret.Data[tokenSecretKey]
		if !ok {
			return fmt.Errorf("token_ref for cluster %q: secret %s/%s has no key %q", cluster.Region, namespace, ref.Name, tokenSecretKey)
		}

		cluster.TokenID = strings.TrimSpace(string(tokenID))
		cluster.TokenSecret = strings.TrimSpace(string(tokenSecret))
	}

	return nil
}
