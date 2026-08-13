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

// Package config is the configuration for the cloud provider.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	yaml "gopkg.in/yaml.v3"

	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"

	"k8s.io/klog/v2"
)

// Provider specifies the provider. Can be 'default' or 'capmox'
type Provider string

const (
	// ProviderDefault is the default provider
	ProviderDefault Provider = "default"

	// ProviderCapmox is the Provider for capmox
	ProviderCapmox Provider = "capmox"
)

const (
	// DefaultControllerVMID is the default VM ID used by the controller when none is specified.
	DefaultControllerVMID = 9999

	// MinControllerVMID is the minimum valid VM ID for the controller.
	MinControllerVMID = 100
)

// ClustersFeatures specifies the features for the cloud provider.
type ClustersFeatures struct {
	// Provider specifies the provider to use. Can be 'default' or 'capmox'.
	// Default is 'default'.
	Provider Provider `yaml:"provider,omitempty"`
	// ControllerVMID is the VM ID used by the controller for volume operations (e.g. volume naming).
	// Default is 9999.
	ControllerVMID int `yaml:"controllerVmID,omitempty"`
	// ReassignVolumeOnAttach enables reassigning a CSI volume's Proxmox ownership (vmid)
	// from ControllerVMID to the real target VM at attach time. Off by default: see
	// docs/reassign-volume-on-attach.md for the VolumeID-stability risk this carries on
	// storages that rename volumes on ownership reassignment (LVM/ZFS/dir).
	ReassignVolumeOnAttach bool `yaml:"reassignVolumeOnAttach,omitempty"`
}

// ClustersConfig is proxmox multi-cluster cloud config.
type ClustersConfig struct {
	Features ClustersFeatures         `yaml:"features,omitempty"`
	Clusters []*pxpool.ProxmoxCluster `yaml:"clusters,omitempty"`
}

// Errors for Reading Cloud Config
var (
	ErrMissingPVERegion       = errors.New("missing PVE region in cloud config")
	ErrMissingPVEAPIURL       = errors.New("missing PVE API URL in cloud config")
	ErrAuthCredentialsMissing = errors.New("user, token or file credentials are required")
	ErrInvalidAuthCredentials = errors.New("must specify one of user, token or file credentials, not multiple")
	ErrInvalidCloudConfig     = errors.New("invalid cloud config")
	ErrInvalidVMID            = errors.New("invalid VM ID, must be greater than 100")
)

// validateCluster checks that a single cluster entry has exactly one valid
// credential source and the required region/URL fields.
func validateCluster(idx int, c *pxpool.ProxmoxCluster) error {
	hasTokenIDInline := c.TokenID != ""
	hasTokenIDFile := c.TokenIDFile != ""
	hasTokenSecretInline := c.TokenSecret != ""
	hasTokenSecretFile := c.TokenSecretFile != ""
	hasTokenRef := c.TokenRef != nil && c.TokenRef.Name != ""

	if (hasTokenIDInline && hasTokenIDFile) || (hasTokenSecretInline && hasTokenSecretFile) {
		return fmt.Errorf("cluster #%d: %w", idx+1, ErrInvalidAuthCredentials)
	}

	if hasTokenRef && (hasTokenIDInline || hasTokenIDFile || hasTokenSecretInline || hasTokenSecretFile) {
		return fmt.Errorf("cluster #%d: %w", idx+1, ErrInvalidAuthCredentials)
	}

	hasTokenID := hasTokenIDInline || hasTokenIDFile || hasTokenRef
	hasTokenSecret := hasTokenSecretInline || hasTokenSecretFile || hasTokenRef

	hasUserAuth := c.Username != "" && c.Password != ""
	if (hasTokenID && hasUserAuth) || (hasTokenSecret && hasUserAuth) {
		return fmt.Errorf("cluster #%d: %w", idx+1, ErrInvalidAuthCredentials)
	}

	if !(hasTokenID && hasTokenSecret) && !hasUserAuth {
		return fmt.Errorf("cluster #%d: %w", idx+1, ErrAuthCredentialsMissing)
	}

	if c.Region == "" {
		return fmt.Errorf("cluster #%d: %w", idx+1, ErrMissingPVERegion)
	}

	if c.URL == "" || !strings.HasPrefix(c.URL, "http") {
		return fmt.Errorf("cluster #%d: %w", idx+1, ErrMissingPVEAPIURL)
	}

	return nil
}

// ReadCloudConfig reads cloud config from a reader.
func ReadCloudConfig(config io.Reader) (ClustersConfig, error) {
	cfg := ClustersConfig{}

	if config != nil {
		if err := yaml.NewDecoder(config).Decode(&cfg); err != nil {
			return ClustersConfig{}, errors.Join(ErrInvalidCloudConfig, err)
		}
	}

	for idx, c := range cfg.Clusters {
		if err := validateCluster(idx, c); err != nil {
			return ClustersConfig{}, err
		}
	}

	if cfg.Features.Provider == "" {
		cfg.Features.Provider = ProviderDefault
	}

	if cfg.Features.ControllerVMID == 0 {
		cfg.Features.ControllerVMID = DefaultControllerVMID
	}

	if cfg.Features.ControllerVMID <= MinControllerVMID {
		return ClustersConfig{}, fmt.Errorf("invalid VM ID, must be greater than %d", MinControllerVMID)
	}

	// A warning rather than an error: the feature degrades to a no-op without the
	// endpoint (the rename fails, the attach proceeds under the volume's existing
	// name), so refusing to start would be a harsher response than the misconfig
	// warrants. But it is silent otherwise, and a flag that is set and does nothing
	// is worth saying out loud once at startup.
	if cfg.Features.ReassignVolumeOnAttach && !hasProxmodEndpoint(cfg.Clusters) {
		klog.Warning("features.reassignVolumeOnAttach is enabled but no cluster sets proxmod_endpoint: true; " +
			"volumes will stay owned by the controller vmid. See docs/reassign-volume-on-attach.md")
	}

	return cfg, nil
}

// hasProxmodEndpoint reports whether any cluster opts into the proxmod extension,
// which is where the rename endpoint reassignVolumeOnAttach depends on lives.
//
// The per-cluster field is read directly rather than through
// ProxmoxPool.ProxmodEndpoint, which needs a built pool this early check does not
// have; the two agree, since a nil override there resolves to the same fallback
// this treats as "not set".
func hasProxmodEndpoint(clusters []*pxpool.ProxmoxCluster) bool {
	for _, c := range clusters {
		if c.ProxmodEndpoint != nil && *c.ProxmodEndpoint {
			return true
		}
	}

	return false
}

// ReadCloudConfigFromFile reads cloud config from a file.
func ReadCloudConfigFromFile(file string) (ClustersConfig, error) {
	f, err := os.Open(filepath.Clean(file))
	if err != nil {
		return ClustersConfig{}, fmt.Errorf("error reading %s: %v", file, err)
	}
	defer f.Close() // nolint: errcheck

	return ReadCloudConfig(f)
}
