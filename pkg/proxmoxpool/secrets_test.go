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

package proxmoxpool_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResolveTokenRefs(t *testing.T) {
	t.Run("resolves token_id/token_secret from the referenced secret", func(t *testing.T) {
		kclient := fake.NewSimpleClientset(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "proxmox-token", Namespace: "kube-system"},
			Data: map[string][]byte{
				"token_id":     []byte("user!token-id"),
				"token_secret": []byte("secret"),
			},
		})

		clusters := []*pxpool.ProxmoxCluster{
			{
				URL:      "https://127.0.0.1:8006/api2/json",
				Region:   "cluster-1",
				TokenRef: &pxpool.TokenRef{Name: "proxmox-token"},
			},
		}

		err := pxpool.ResolveTokenRefs(context.Background(), kclient, "kube-system", clusters)
		assert.NoError(t, err)
		assert.Equal(t, "user!token-id", clusters[0].TokenID)
		assert.Equal(t, "secret", clusters[0].TokenSecret)
	})

	t.Run("uses custom key names and explicit namespace", func(t *testing.T) {
		kclient := fake.NewSimpleClientset(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "proxmox-token", Namespace: "custom-ns"},
			Data: map[string][]byte{
				"id":     []byte("user!token-id"),
				"secret": []byte("s3cr3t"),
			},
		})

		clusters := []*pxpool.ProxmoxCluster{
			{
				URL:    "https://127.0.0.1:8006/api2/json",
				Region: "cluster-1",
				TokenRef: &pxpool.TokenRef{
					Name:           "proxmox-token",
					Namespace:      "custom-ns",
					TokenIDKey:     "id",
					TokenSecretKey: "secret",
				},
			},
		}

		err := pxpool.ResolveTokenRefs(context.Background(), kclient, "kube-system", clusters)
		assert.NoError(t, err)
		assert.Equal(t, "user!token-id", clusters[0].TokenID)
		assert.Equal(t, "s3cr3t", clusters[0].TokenSecret)
	})

	t.Run("leaves clusters without a token_ref untouched", func(t *testing.T) {
		kclient := fake.NewSimpleClientset()

		clusters := []*pxpool.ProxmoxCluster{
			{
				URL:         "https://127.0.0.1:8006/api2/json",
				Region:      "cluster-1",
				TokenID:     "user!token-id",
				TokenSecret: "secret",
			},
		}

		err := pxpool.ResolveTokenRefs(context.Background(), kclient, "kube-system", clusters)
		assert.NoError(t, err)
		assert.Equal(t, "user!token-id", clusters[0].TokenID)
		assert.Equal(t, "secret", clusters[0].TokenSecret)
	})

	t.Run("errors when no namespace is available", func(t *testing.T) {
		kclient := fake.NewSimpleClientset()

		clusters := []*pxpool.ProxmoxCluster{
			{
				URL:      "https://127.0.0.1:8006/api2/json",
				Region:   "cluster-1",
				TokenRef: &pxpool.TokenRef{Name: "proxmox-token"},
			},
		}

		err := pxpool.ResolveTokenRefs(context.Background(), kclient, "", clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "namespace is required")
	})

	t.Run("errors when the secret does not exist", func(t *testing.T) {
		kclient := fake.NewSimpleClientset()

		clusters := []*pxpool.ProxmoxCluster{
			{
				URL:      "https://127.0.0.1:8006/api2/json",
				Region:   "cluster-1",
				TokenRef: &pxpool.TokenRef{Name: "missing-token"},
			},
		}

		err := pxpool.ResolveTokenRefs(context.Background(), kclient, "kube-system", clusters)
		assert.Error(t, err)
	})

	t.Run("errors when a key is missing from the secret", func(t *testing.T) {
		kclient := fake.NewSimpleClientset(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "proxmox-token", Namespace: "kube-system"},
			Data: map[string][]byte{
				"token_id": []byte("user!token-id"),
			},
		})

		clusters := []*pxpool.ProxmoxCluster{
			{
				URL:      "https://127.0.0.1:8006/api2/json",
				Region:   "cluster-1",
				TokenRef: &pxpool.TokenRef{Name: "proxmox-token"},
			},
		}

		err := pxpool.ResolveTokenRefs(context.Background(), kclient, "kube-system", clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "token_secret")
	})
}
