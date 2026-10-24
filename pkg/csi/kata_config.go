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

package csi

const (
	// DefaultKataDirectVolumeRoot is where Kata looks for direct-assigned volume descriptions.
	DefaultKataDirectVolumeRoot = "/run/kata-containers/shared/direct-volumes"
	// DefaultKataHandlerPrefix marks RuntimeClass handlers that run pods in Kata VMs.
	DefaultKataHandlerPrefix = "kata"
)

// KataDirectVolumes configures direct-assigned volumes for Kata pods.
type KataDirectVolumes struct {
	// Enabled publishes filesystem volumes of Kata pods as direct-assigned volumes.
	Enabled bool
	// HandlerPrefix marks Kata RuntimeClass handlers.
	HandlerPrefix string
	// Root is Kata's direct volume directory.
	Root string
}
