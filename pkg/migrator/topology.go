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

package migrator

import (
	"context"
	"fmt"

	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// migrationAnnotations are stripped from the recreated PVC/PV so a completed
// migration cannot re-trigger itself.
var migrationAnnotations = []string{
	AnnotationMigrate,
	AnnotationMigrateNode,
	AnnotationMigrateForce,
	AnnotationMigrateStorage,
	AnnotationMigratePhase,
	AnnotationMigrateMessage,
	AnnotationMigrateAttempts,
	AnnotationMigrateStartedAt,
	AnnotationMigrateState,
}

// replacePVTopology deletes the PV/PVC pair and recreates them with the
// target volume's node and storage in the CSI volume handle and the
// node-affinity topology.
func (m *Migrator) replacePVTopology(
	ctx context.Context,
	namespace string,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
	targetVol *volume.Volume,
) error {
	newPVC := pvc.DeepCopy()
	newPVC.ObjectMeta.UID = ""
	newPVC.ObjectMeta.ResourceVersion = ""
	newPVC.ObjectMeta.DeletionTimestamp = nil
	newPVC.ObjectMeta.DeletionGracePeriodSeconds = nil

	for _, a := range migrationAnnotations {
		delete(newPVC.ObjectMeta.Annotations, a)
	}

	newPVC.Status = corev1.PersistentVolumeClaimStatus{}
	newPVC.Spec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceStorage: pvc.Status.Capacity[corev1.ResourceStorage],
	}

	newPV := pv.DeepCopy()
	newPV.ObjectMeta.UID = ""
	newPV.ObjectMeta.ResourceVersion = ""
	newPV.ObjectMeta.DeletionTimestamp = nil
	newPV.ObjectMeta.DeletionGracePeriodSeconds = nil

	for _, a := range migrationAnnotations {
		delete(newPV.ObjectMeta.Annotations, a)
	}

	newPV.Spec.ClaimRef = nil
	newPV.Status = corev1.PersistentVolumeStatus{}
	newPV.Spec.CSI.VolumeHandle = targetVol.VolumeID()
	newPV.Spec.NodeAffinity.Required = &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      corev1.LabelTopologyRegion,
						Operator: "In",
						Values:   []string{targetVol.Region()},
					},
					{
						Key:      corev1.LabelTopologyZone,
						Operator: "In",
						Values:   []string{targetVol.Zone()},
					},
				},
			},
		},
	}

	policy := metav1.DeletePropagationForeground
	if err := m.KClient.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		return fmt.Errorf("failed to delete PVC: %v", err)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		if err := m.KClient.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
			return fmt.Errorf("failed to delete PV: %v", err)
		}
	}

	if err := tools.PVWaitDelete(ctx, m.KClient, pv.Name); err != nil {
		return fmt.Errorf("failed to wait for PV deletion: %v", err)
	}

	if _, err := m.KClient.CoreV1().PersistentVolumes().Create(ctx, newPV, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create PV: %v", err)
	}

	if _, err := m.KClient.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, newPVC, metav1.CreateOptions{}); err != nil {
		if _, err := m.KClient.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, newPVC, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to create/update PVC: %v", err)
		}
	}

	return nil
}
