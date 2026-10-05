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

package api

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	volumev1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/volume/v1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	volctrl "github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/volume"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	utilsnode "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/node"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumeIDIndex is the field index on ProxmoxVolume for status.volumeID lookups.
const VolumeIDIndex = "volumeID"

// SnapshotIDIndex is the field index on ProxmoxVolumeSnapshot for status.snapshotID lookups.
const SnapshotIDIndex = "snapshotID"

// IndexVolumeID is the index function for VolumeIDIndex.
func IndexVolumeID(obj client.Object) []string {
	vol, ok := obj.(*v1alpha1.ProxmoxVolume)
	if !ok {
		return nil
	}

	if vol.Status.VolumeID != "" {
		return []string{vol.Status.VolumeID}
	}

	return nil
}

// IndexSnapshotID is the index function for SnapshotIDIndex.
func IndexSnapshotID(obj client.Object) []string {
	snap, ok := obj.(*v1alpha1.ProxmoxVolumeSnapshot)
	if !ok {
		return nil
	}

	if snap.Status.SnapshotID != "" {
		return []string{snap.Status.SnapshotID}
	}

	return nil
}

// Service implements the volume gRPC API.
type Service struct {
	volumev1.UnimplementedVolumeServiceServer

	client client.Client
	vms    proxmox.VMReader
}

// The permissions this API needs on behalf of the operator SA.
//
//+kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes/status,verbs=get
//+kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumeattachments,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumesnapshots,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxstorages,verbs=get;list;watch

// NewService creates a new volume API service.
func NewService(c client.Client, vms proxmox.VMReader) *Service {
	return &Service{client: c, vms: vms}
}

// CreateVolume creates or gets a ProxmoxVolume CR and returns once it is Ready.
func (s *Service) CreateVolume(ctx context.Context, req *volumev1.CreateVolumeRequest) (*volumev1.CreateVolumeResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	if req.CapacityBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity_bytes must be positive")
	}

	if req.Region == "" {
		return nil, status.Error(codes.InvalidArgument, "region is required")
	}

	if req.Storage == "" {
		return nil, status.Error(codes.InvalidArgument, "storage is required")
	}

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	requestID := RequestIDFromContext(ctx)
	namespace := tenant.Spec.Namespace

	vol := &v1alpha1.ProxmoxVolume{}
	key := client.ObjectKey{Namespace: namespace, Name: req.Name}

	err := s.client.Get(ctx, key, vol)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "getting volume: %v", err)
	}

	if apierrors.IsNotFound(err) {
		// Build the CR.
		vol = &v1alpha1.ProxmoxVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      req.Name,
				Namespace: namespace,
				Labels: map[string]string{
					v1alpha1.LabelTenant: tenant.Name,
				},
				Annotations: map[string]string{
					v1alpha1.GroupName + "/request-id": requestID,
				},
			},
			Spec: v1alpha1.ProxmoxVolumeSpec{
				Region:            req.Region,
				Zone:              req.Zone,
				Storage:           req.Storage,
				CapacityBytes:     req.CapacityBytes,
				Parameters:        req.Parameters,
				MutableParameters: req.MutableParameters,
			},
		}

		if req.ClaimRef != nil {
			vol.Spec.ClaimRef = v1alpha1.TenantClaimRef{
				Name:      req.ClaimRef.Name,
				Namespace: req.ClaimRef.Namespace,
				PVName:    req.Name,
			}
		}

		if req.Source != nil {
			vol.Spec.Source = &v1alpha1.VolumeSource{
				VolumeName:   req.Source.VolumeName,
				SnapshotName: req.Source.SnapshotName,
			}
		}

		if createErr := s.client.Create(ctx, vol); createErr != nil {
			if apierrors.IsAlreadyExists(createErr) {
				// Re-read: race with another request.
				if getErr := s.client.Get(ctx, key, vol); getErr != nil {
					return nil, status.Errorf(codes.Internal, "getting volume after create race: %v", getErr)
				}
			} else {
				return nil, status.Errorf(codes.Internal, "creating volume: %v", createErr)
			}
		}
	} else if !specMatchesRequest(vol, req) {
		return nil, status.Errorf(codes.AlreadyExists, "volume %s exists with a different spec", req.Name)
	}

	// Check status phase.
	switch vol.Status.Phase { //nolint:exhaustive // default handles the remaining phases.
	case v1alpha1.VolumePhaseReady:
		// The remote controller sends `storage` as its own field, not a parameter;
		// direct mode's context carries it, so put it back before building one.
		parameters := maps.Clone(vol.Spec.Parameters)
		if parameters == nil {
			parameters = map[string]string{}
		}

		parameters["storage"] = vol.Spec.Storage

		volumeContext, err := csi.VolumeContext(parameters, vol.Spec.MutableParameters)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "volume context: %v", err)
		}

		return &volumev1.CreateVolumeResponse{
			VolumeId:           vol.Status.VolumeID,
			CapacityBytes:      vol.Status.CapacityBytes,
			AccessibleTopology: vol.Status.AccessibleTopology,
			VolumeContext:      volumeContext,
		}, nil

	case v1alpha1.VolumePhaseRejected:
		return nil, mapRejectionCode(vol)

	case v1alpha1.VolumePhaseFailed:
		msg := "provisioning failed"
		if cond := apimeta.FindStatusCondition(vol.Status.Conditions, v1alpha1.ConditionReady); cond != nil {
			msg = cond.Message
		}

		// Delete the failed CR so the next retry from the sidecar can create
		// a fresh one. Return a final error (Internal) so external-provisioner
		// stops retrying this PV name.
		if vol.DeletionTimestamp.IsZero() {
			if delErr := s.client.Delete(ctx, vol); delErr != nil && !apierrors.IsNotFound(delErr) {
				return nil, status.Errorf(codes.Internal, "cleaning up failed volume: %v", delErr)
			}
		}

		// Not Aborted: the CSI sidecars read Aborted (like Unavailable) as "still in
		// progress" and retry, which is exactly what this must stop. Internal is final.
		return nil, status.Errorf(codes.Internal, "provisioning failed: %s", msg)

	default:
		// Pending or other -- provisioning in progress.
		return nil, status.Errorf(codes.Unavailable, "provisioning volume %s", vol.Name)
	}
}

// DeleteVolume deletes the ProxmoxVolume by volumeID and waits for cleanup.
func (s *Service) DeleteVolume(ctx context.Context, req *volumev1.DeleteVolumeRequest) (*volumev1.DeleteVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	namespace := tenant.Spec.Namespace

	// Find the volume by status.volumeID in the tenant's namespace.
	list := &v1alpha1.ProxmoxVolumeList{}
	if err := s.client.List(ctx, list,
		client.InNamespace(namespace),
		client.MatchingFields{VolumeIDIndex: req.VolumeId},
	); err != nil {
		return nil, status.Errorf(codes.Internal, "listing volumes: %v", err)
	}

	if len(list.Items) == 0 {
		// Idempotent: not found = success.
		return &volumev1.DeleteVolumeResponse{}, nil
	}

	vol := &list.Items[0]

	// Delete the object if not already being deleted.
	if vol.DeletionTimestamp.IsZero() {
		if err := s.client.Delete(ctx, vol); err != nil {
			if apierrors.IsNotFound(err) {
				return &volumev1.DeleteVolumeResponse{}, nil
			}

			return nil, status.Errorf(codes.Internal, "deleting volume: %v", err)
		}
	}

	// Check if it still exists (finalizer running).
	key := client.ObjectKeyFromObject(vol)
	if err := s.client.Get(ctx, key, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return &volumev1.DeleteVolumeResponse{}, nil
		}

		return nil, status.Errorf(codes.Internal, "checking volume deletion: %v", err)
	}

	// Still exists -- finalizer is running.
	return nil, status.Errorf(codes.Unavailable, "deleting volume %s", vol.Name)
}

// WatchCapacity streams ProxmoxStorage objects filtered to the tenant's
// allowedStorages.
func (s *Service) WatchCapacity(req *volumev1.WatchCapacityRequest, stream grpc.ServerStreamingServer[volumev1.WatchCapacityResponse]) error {
	ctx := stream.Context()

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return status.Error(codes.Internal, "tenant not in context")
	}

	logger := log.FromContext(ctx)

	// Send current state, then poll periodically.
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		if err := s.sendCapacity(ctx, tenant, req, stream); err != nil {
			logger.Error(err, "sending capacity")

			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// specMatchesRequest checks whether an existing volume's spec matches the create
// request.
func specMatchesRequest(vol *v1alpha1.ProxmoxVolume, req *volumev1.CreateVolumeRequest) bool {
	if vol.Spec.Region != req.Region {
		return false
	}

	if vol.Spec.Zone != req.Zone {
		return false
	}

	if vol.Spec.Storage != req.Storage {
		return false
	}

	if vol.Spec.CapacityBytes != req.CapacityBytes {
		return false
	}

	return true
}

// mapRejectionCode maps a Rejected volume's condition reason to a gRPC code.
func mapRejectionCode(vol *v1alpha1.ProxmoxVolume) error {
	cond := apimeta.FindStatusCondition(vol.Status.Conditions, v1alpha1.ConditionAdmitted)

	reason := ""
	message := "volume rejected"

	if cond != nil {
		reason = cond.Reason
		message = cond.Message
	}

	switch reason {
	case v1alpha1.ReasonQuotaExceeded, v1alpha1.ReasonNamespaceQuota:
		return status.Error(codes.ResourceExhausted, message)
	case v1alpha1.ReasonTenantSuspended, v1alpha1.ReasonTenantObserveOnly,
		v1alpha1.ReasonStorageNotAllowed, v1alpha1.ReasonVMIDNotOwned:
		return status.Error(codes.PermissionDenied, message)
	default:
		return status.Error(codes.InvalidArgument, message)
	}
}

// AttachVolume creates or gets a ProxmoxVolumeAttachment and returns once it is
// attached.
func (s *Service) AttachVolume(ctx context.Context, req *volumev1.AttachVolumeRequest) (*volumev1.AttachVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	// req.MutableParameters is deliberately ignored. The disk options the
	// attachment reconciler passes to Proxmox come from the operator's own
	// record of the volume (spec.parameters + spec.mutableParameters), not
	// from the tenant's request. VolumeAttributesClass changes reach the
	// volume through ModifyVolume (Phase 3), which applies ParameterPolicy.

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	requestID := RequestIDFromContext(ctx)
	namespace := tenant.Spec.Namespace

	// Find the volume by status.volumeID.
	vol, err := s.volumeByID(ctx, namespace, req.VolumeId)
	if err != nil {
		return nil, err
	}

	if vol.Status.Phase != v1alpha1.VolumePhaseReady {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is not ready (phase %s)", vol.Name, vol.Status.Phase)
	}

	// Resolve VMID -- never trust the tenant.
	vmid, err := s.resolveVMID(ctx, tenant, req.NodeId, req.Vmid)
	if err != nil {
		return nil, err
	}

	// Deterministic name: "pva-" + first 16 hex of sha256(volumeID + "\x00" + nodeID).
	attName := attachmentName(req.VolumeId, req.NodeId)

	att := &v1alpha1.ProxmoxVolumeAttachment{}
	key := client.ObjectKey{Namespace: namespace, Name: attName}

	if getErr := s.client.Get(ctx, key, att); getErr != nil {
		if !apierrors.IsNotFound(getErr) {
			return nil, status.Errorf(codes.Internal, "getting attachment: %v", getErr)
		}

		// Create the attachment.
		att = &v1alpha1.ProxmoxVolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      attName,
				Namespace: namespace,
				Labels: map[string]string{
					v1alpha1.LabelTenant: tenant.Name,
				},
				Annotations: map[string]string{
					v1alpha1.GroupName + "/request-id": requestID,
				},
			},
			Spec: v1alpha1.ProxmoxVolumeAttachmentSpec{
				VolumeName: vol.Name,
				VolumeID:   req.VolumeId,
				NodeID:     req.NodeId,
				VMID:       vmid,
				Readonly:   req.Readonly,
			},
		}

		if createErr := s.client.Create(ctx, att); createErr != nil {
			if apierrors.IsAlreadyExists(createErr) {
				if getErr := s.client.Get(ctx, key, att); getErr != nil {
					return nil, status.Errorf(codes.Internal, "getting attachment after create race: %v", getErr)
				}
			} else {
				return nil, status.Errorf(codes.Internal, "creating attachment: %v", createErr)
			}
		}
	}

	// Immutable spec mismatch: the attachment exists but for a different spec.
	if att.Spec.VolumeID != req.VolumeId || att.Spec.NodeID != req.NodeId || att.Spec.VMID != vmid {
		return nil, status.Errorf(codes.AlreadyExists,
			"attachment %s exists with a different spec (volumeID=%s nodeID=%s vmid=%d)",
			attName, att.Spec.VolumeID, att.Spec.NodeID, att.Spec.VMID)
	}

	if !att.Status.Attached {
		// Distinguish transient from permanent failures.
		if cond := apimeta.FindStatusCondition(att.Status.Conditions, v1alpha1.ConditionAdmitted); cond != nil && cond.Status == metav1.ConditionFalse {
			// Permanent refusal (wrong VMID, volume mismatch, etc.). Delete the
			// failed attachment so the sidecar can retry with correct parameters.
			msg := cond.Message

			if att.DeletionTimestamp.IsZero() {
				if delErr := s.client.Delete(ctx, att); delErr != nil && !apierrors.IsNotFound(delErr) {
					return nil, status.Errorf(codes.Internal, "cleaning up refused attachment: %v", delErr)
				}
			}

			return nil, status.Errorf(codes.Internal, "attach refused: %s", msg)
		}

		return nil, status.Errorf(codes.Unavailable, "attaching volume %s to VM %d", vol.Name, vmid)
	}

	return &volumev1.AttachVolumeResponse{
		DevicePath:     att.Status.DevicePath,
		Lun:            lunFromStatus(att.Status.LUN),
		ResizeRequired: att.Status.ResizeRequired,
	}, nil
}

// DetachVolume deletes a ProxmoxVolumeAttachment and waits for it to disappear.
func (s *Service) DetachVolume(ctx context.Context, req *volumev1.DetachVolumeRequest) (*volumev1.DetachVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	namespace := tenant.Spec.Namespace
	attName := attachmentName(req.VolumeId, req.NodeId)

	att := &v1alpha1.ProxmoxVolumeAttachment{}
	key := client.ObjectKey{Namespace: namespace, Name: attName}

	if err := s.client.Get(ctx, key, att); err != nil {
		if apierrors.IsNotFound(err) {
			return &volumev1.DetachVolumeResponse{}, nil
		}

		return nil, status.Errorf(codes.Internal, "getting attachment: %v", err)
	}

	// Delete the object if not already being deleted.
	if att.DeletionTimestamp.IsZero() {
		if err := s.client.Delete(ctx, att); err != nil {
			if apierrors.IsNotFound(err) {
				return &volumev1.DetachVolumeResponse{}, nil
			}

			return nil, status.Errorf(codes.Internal, "deleting attachment: %v", err)
		}
	}

	// Check if it still exists (finalizer running).
	if err := s.client.Get(ctx, key, att); err != nil {
		if apierrors.IsNotFound(err) {
			return &volumev1.DetachVolumeResponse{}, nil
		}

		return nil, status.Errorf(codes.Internal, "checking attachment deletion: %v", err)
	}

	return nil, status.Errorf(codes.Unavailable, "detaching volume %s", req.VolumeId)
}

// ExpandVolume patches spec.capacityBytes upward and returns once the reconciler
// has resized the disk.
func (s *Service) ExpandVolume(ctx context.Context, req *volumev1.ExpandVolumeRequest) (*volumev1.ExpandVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	if req.CapacityBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity_bytes must be positive")
	}

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	namespace := tenant.Spec.Namespace

	vol, err := s.volumeByID(ctx, namespace, req.VolumeId)
	if err != nil {
		return nil, err
	}

	if vol.Status.Phase != v1alpha1.VolumePhaseReady {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is not ready (phase %s)", vol.Name, vol.Status.Phase)
	}

	// Refuse shrink. If the requested size is at or below the current size,
	// return the current state idempotently.
	if req.CapacityBytes <= vol.Status.CapacityBytes {
		return &volumev1.ExpandVolumeResponse{
			CapacityBytes:         vol.Status.CapacityBytes,
			NodeExpansionRequired: true,
		}, nil
	}

	// Check maxVolumeBytes from the tenant's parameter policy.
	if tenant.Spec.ParameterPolicy != nil && tenant.Spec.ParameterPolicy.MaxVolumeBytes != nil {
		maxBytes := tenant.Spec.ParameterPolicy.MaxVolumeBytes.Value()
		if req.CapacityBytes > maxBytes {
			return nil, status.Errorf(codes.ResourceExhausted,
				"requested capacity %d exceeds maxVolumeBytes %d", req.CapacityBytes, maxBytes)
		}
	}

	// Patch capacityBytes upward. The CEL rule `self >= oldSelf` on the field
	// enforces the no-shrink invariant at admission too.
	if vol.Spec.CapacityBytes < req.CapacityBytes {
		vol.Spec.CapacityBytes = req.CapacityBytes
		if err := s.client.Update(ctx, vol); err != nil {
			return nil, status.Errorf(codes.Internal, "updating volume capacity: %v", err)
		}
	}

	// Re-read to check whether the reconciler has caught up.
	key := client.ObjectKeyFromObject(vol)
	if err := s.client.Get(ctx, key, vol); err != nil {
		return nil, status.Errorf(codes.Internal, "re-reading volume: %v", err)
	}

	if vol.Status.CapacityBytes >= req.CapacityBytes {
		return &volumev1.ExpandVolumeResponse{
			CapacityBytes:         vol.Status.CapacityBytes,
			NodeExpansionRequired: true,
		}, nil
	}

	return nil, status.Errorf(codes.Unavailable, "expanding volume %s", vol.Name)
}

// ModifyVolume patches the mutable parameters on a volume.
func (s *Service) ModifyVolume(ctx context.Context, req *volumev1.ModifyVolumeRequest) (*volumev1.ModifyVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	namespace := tenant.Spec.Namespace

	vol, err := s.volumeByID(ctx, namespace, req.VolumeId)
	if err != nil {
		return nil, err
	}

	if vol.Status.Phase != v1alpha1.VolumePhaseReady {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is not ready (phase %s)", vol.Name, vol.Status.Phase)
	}

	// Validate mutable parameters against the tenant's parameter policy.
	if tenant.Spec.ParameterPolicy != nil && tenant.Spec.ParameterPolicy.Allowed != nil {
		for key := range req.MutableParameters {
			if !slices.Contains(tenant.Spec.ParameterPolicy.Allowed, key) {
				return nil, status.Errorf(codes.InvalidArgument, "mutable parameter %q is not in the allowed list", key)
			}
		}
	}

	if tenant.Spec.ParameterPolicy != nil {
		for key, max := range tenant.Spec.ParameterPolicy.MaxInt {
			val, ok := req.MutableParameters[key]
			if !ok {
				continue
			}

			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "mutable parameter %q must be an integer: %v", key, err)
			}

			if n > max {
				return nil, status.Errorf(codes.InvalidArgument, "mutable parameter %q value %d exceeds maximum %d", key, n, max)
			}
		}
	}

	vol.Spec.MutableParameters = req.MutableParameters
	if err := s.client.Update(ctx, vol); err != nil {
		return nil, status.Errorf(codes.Internal, "updating volume mutable parameters: %v", err)
	}

	// Check if the volume is attached. If so, return Unavailable until the
	// reconciler applies the options to the live disk. If detached, return OK
	// immediately -- same as direct mode.
	attachments := &v1alpha1.ProxmoxVolumeAttachmentList{}
	if err := s.client.List(ctx, attachments, client.InNamespace(namespace)); err != nil {
		return nil, status.Errorf(codes.Internal, "listing attachments: %v", err)
	}

	for i := range attachments.Items {
		att := &attachments.Items[i]
		if att.Spec.VolumeName == vol.Name && att.Status.Attached {
			// Volume is attached. Re-read to check whether the reconciler has
			// already applied the new parameters.
			key := client.ObjectKeyFromObject(vol)
			if err := s.client.Get(ctx, key, vol); err != nil {
				return nil, status.Errorf(codes.Internal, "re-reading volume: %v", err)
			}

			appliedHash := vol.Annotations[volctrl.AppliedMutableParamsAnnotation]
			currentHash := volctrl.MutableParamsHash(req.MutableParameters)

			if appliedHash == currentHash {
				return &volumev1.ModifyVolumeResponse{}, nil
			}

			return nil, status.Errorf(codes.Unavailable, "modifying volume %s", vol.Name)
		}
	}

	return &volumev1.ModifyVolumeResponse{}, nil
}

// CreateSnapshot creates or gets a ProxmoxVolumeSnapshot and returns once the
// snapshot ID is set.
func (s *Service) CreateSnapshot(ctx context.Context, req *volumev1.CreateSnapshotRequest) (*volumev1.CreateSnapshotResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	if req.SourceVolumeName == "" {
		return nil, status.Error(codes.InvalidArgument, "source_volume_name is required")
	}

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	requestID := RequestIDFromContext(ctx)
	namespace := tenant.Spec.Namespace

	// Verify the source volume exists and is ready.
	srcVol := &v1alpha1.ProxmoxVolume{}
	srcKey := client.ObjectKey{Namespace: namespace, Name: req.SourceVolumeName}

	if err := s.client.Get(ctx, srcKey, srcVol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "source volume %s not found", req.SourceVolumeName)
		}

		return nil, status.Errorf(codes.Internal, "getting source volume: %v", err)
	}

	if srcVol.Status.Phase != v1alpha1.VolumePhaseReady {
		return nil, status.Errorf(codes.FailedPrecondition, "source volume %s is not ready (phase %s)",
			req.SourceVolumeName, srcVol.Status.Phase)
	}

	snap := &v1alpha1.ProxmoxVolumeSnapshot{}
	key := client.ObjectKey{Namespace: namespace, Name: req.Name}

	err := s.client.Get(ctx, key, snap)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "getting snapshot: %v", err)
	}

	if apierrors.IsNotFound(err) {
		snap = &v1alpha1.ProxmoxVolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      req.Name,
				Namespace: namespace,
				Labels: map[string]string{
					v1alpha1.LabelTenant: tenant.Name,
				},
				Annotations: map[string]string{
					v1alpha1.GroupName + "/request-id": requestID,
				},
			},
			Spec: v1alpha1.ProxmoxVolumeSnapshotSpec{
				SourceVolumeName: req.SourceVolumeName,
				Storage:          srcVol.Spec.Storage,
				Zone:             req.Zone,
			},
		}

		if createErr := s.client.Create(ctx, snap); createErr != nil {
			if apierrors.IsAlreadyExists(createErr) {
				if getErr := s.client.Get(ctx, key, snap); getErr != nil {
					return nil, status.Errorf(codes.Internal, "getting snapshot after create race: %v", getErr)
				}
			} else {
				return nil, status.Errorf(codes.Internal, "creating snapshot: %v", createErr)
			}
		}
	} else if snap.Spec.SourceVolumeName != req.SourceVolumeName {
		return nil, status.Errorf(codes.AlreadyExists, "snapshot %s exists with a different source", req.Name)
	}

	// Check if the snapshot completed successfully.
	if snap.Status.SnapshotID != "" && snap.Status.ReadyToUse {
		return &volumev1.CreateSnapshotResponse{
			SnapshotId:       snap.Status.SnapshotID,
			ReadyToUse:       snap.Status.ReadyToUse,
			RestoreSizeBytes: snap.Status.RestoreSizeBytes,
			CreationTime:     timestampFromMeta(snap.Status.CreationTime),
		}, nil
	}

	// Check for a persistent failure: the reconciler set ProxmoxError but
	// readyToUse is still false. Delete the failed CR so a retry from the
	// sidecar creates a fresh one, and return a final error so
	// external-snapshotter stops retrying this name.
	if cond := apimeta.FindStatusCondition(snap.Status.Conditions, v1alpha1.ConditionReady); cond != nil {
		if cond.Status == metav1.ConditionFalse && cond.Reason == v1alpha1.ReasonProxmoxError {
			msg := cond.Message

			if snap.DeletionTimestamp.IsZero() {
				if delErr := s.client.Delete(ctx, snap); delErr != nil && !apierrors.IsNotFound(delErr) {
					return nil, status.Errorf(codes.Internal, "cleaning up failed snapshot: %v", delErr)
				}
			}

			return nil, status.Errorf(codes.Internal, "snapshot failed: %s", msg)
		}
	}

	return nil, status.Errorf(codes.Unavailable, "creating snapshot %s", snap.Name)
}

// DeleteSnapshot deletes a ProxmoxVolumeSnapshot by snapshotID and waits for
// cleanup.
func (s *Service) DeleteSnapshot(ctx context.Context, req *volumev1.DeleteSnapshotRequest) (*volumev1.DeleteSnapshotResponse, error) {
	if req.SnapshotId == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}

	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	namespace := tenant.Spec.Namespace

	snap, err := s.snapshotByID(ctx, namespace, req.SnapshotId)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// Idempotent: not found = success.
			return &volumev1.DeleteSnapshotResponse{}, nil
		}

		return nil, err
	}

	if snap.DeletionTimestamp.IsZero() {
		if err := s.client.Delete(ctx, snap); err != nil {
			if apierrors.IsNotFound(err) {
				return &volumev1.DeleteSnapshotResponse{}, nil
			}

			return nil, status.Errorf(codes.Internal, "deleting snapshot: %v", err)
		}
	}

	key := client.ObjectKeyFromObject(snap)
	if err := s.client.Get(ctx, key, snap); err != nil {
		if apierrors.IsNotFound(err) {
			return &volumev1.DeleteSnapshotResponse{}, nil
		}

		return nil, status.Errorf(codes.Internal, "checking snapshot deletion: %v", err)
	}

	return nil, status.Errorf(codes.Unavailable, "deleting snapshot %s", snap.Name)
}

// ListSnapshots returns the tenant's own snapshots.
func (s *Service) ListSnapshots(ctx context.Context, req *volumev1.ListSnapshotsRequest) (*volumev1.ListSnapshotsResponse, error) {
	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil, status.Error(codes.Internal, "tenant not in context")
	}

	namespace := tenant.Spec.Namespace

	// If a specific snapshot ID is requested, return just that one.
	if req.SnapshotId != "" {
		snap, err := s.snapshotByID(ctx, namespace, req.SnapshotId)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return &volumev1.ListSnapshotsResponse{}, nil
			}

			return nil, err
		}

		return &volumev1.ListSnapshotsResponse{
			Entries: []*volumev1.ListSnapshotsResponse_Entry{snapshotEntry(snap)},
		}, nil
	}

	list := &v1alpha1.ProxmoxVolumeSnapshotList{}
	if err := s.client.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, status.Errorf(codes.Internal, "listing snapshots: %v", err)
	}

	// Filter by source volume name if requested.
	var filtered []v1alpha1.ProxmoxVolumeSnapshot

	for i := range list.Items {
		snap := &list.Items[i]

		if snap.Status.SnapshotID == "" {
			continue
		}

		if req.SourceVolumeName != "" && snap.Spec.SourceVolumeName != req.SourceVolumeName {
			continue
		}

		filtered = append(filtered, *snap)
	}

	// Pagination.
	start := 0

	if req.StartingToken != "" {
		n, err := strconv.Atoi(req.StartingToken)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid starting_token: %v", err)
		}

		start = n
	}

	if start > len(filtered) {
		start = len(filtered)
	}

	remaining := filtered[start:]
	maxEntries := int(req.MaxEntries)

	if maxEntries <= 0 {
		maxEntries = len(remaining)
	}

	if maxEntries > len(remaining) {
		maxEntries = len(remaining)
	}

	entries := make([]*volumev1.ListSnapshotsResponse_Entry, 0, maxEntries)
	for i := range remaining[:maxEntries] {
		entries = append(entries, snapshotEntry(&remaining[i]))
	}

	resp := &volumev1.ListSnapshotsResponse{Entries: entries}

	if start+maxEntries < len(filtered) {
		resp.NextToken = strconv.Itoa(start + maxEntries)
	}

	return resp, nil
}

// snapshotByID finds a ProxmoxVolumeSnapshot by status.snapshotID in a namespace.
//
//nolint:dupl // Structurally identical to volumeByID but operates on a different type and index.
func (s *Service) snapshotByID(ctx context.Context, namespace, snapshotID string) (*v1alpha1.ProxmoxVolumeSnapshot, error) {
	list := &v1alpha1.ProxmoxVolumeSnapshotList{}
	if err := s.client.List(ctx, list,
		client.InNamespace(namespace),
		client.MatchingFields{SnapshotIDIndex: snapshotID},
	); err != nil {
		return nil, status.Errorf(codes.Internal, "listing snapshots: %v", err)
	}

	if len(list.Items) == 0 {
		return nil, status.Errorf(codes.NotFound, "snapshot %s not found in namespace %s", snapshotID, namespace)
	}

	return &list.Items[0], nil
}

// snapshotEntry converts a ProxmoxVolumeSnapshot to a ListSnapshots entry.
func snapshotEntry(snap *v1alpha1.ProxmoxVolumeSnapshot) *volumev1.ListSnapshotsResponse_Entry {
	// Look up the source volume ID from the snapshot's source.
	// The snapshot's snapshotID carries the same format as a volumeID, and the
	// source volume ID is not stored on the snapshot status. We return an
	// empty string here; the caller (remote controller) has the source from
	// the CSI side.
	return &volumev1.ListSnapshotsResponse_Entry{
		SnapshotId:       snap.Status.SnapshotID,
		SourceVolumeId:   "", // Not tracked on the snapshot; the CSI sidecar knows its source.
		ReadyToUse:       snap.Status.ReadyToUse,
		RestoreSizeBytes: snap.Status.RestoreSizeBytes,
		CreationTime:     timestampFromMeta(snap.Status.CreationTime),
	}
}

// timestampFromMeta converts a Kubernetes metav1.Time to a protobuf Timestamp.
func timestampFromMeta(t *metav1.Time) *timestamppb.Timestamp {
	if t == nil || t.IsZero() {
		return nil
	}

	return timestamppb.New(t.Time)
}

func (s *Service) sendCapacity(
	ctx context.Context,
	tenant *v1alpha1.TenantCluster,
	req *volumev1.WatchCapacityRequest,
	stream grpc.ServerStreamingServer[volumev1.WatchCapacityResponse],
) error {
	list := &v1alpha1.ProxmoxStorageList{}
	if err := s.client.List(ctx, list); err != nil {
		return status.Errorf(codes.Internal, "listing storages: %v", err)
	}

	for i := range list.Items {
		ps := &list.Items[i]

		// Filter to tenant's allowed storages.
		if !slices.Contains(tenant.Spec.AllowedStorages, ps.Spec.Storage) {
			continue
		}

		// Apply request filters.
		if req.Storage != "" && ps.Spec.Storage != req.Storage {
			continue
		}

		if req.Zone != "" && ps.Spec.Zone != req.Zone {
			continue
		}

		resp := &volumev1.WatchCapacityResponse{
			Storage:        ps.Spec.Storage,
			Zone:           ps.Spec.Zone,
			AvailableBytes: ps.Status.AvailableBytes,
			TotalBytes:     ps.Status.TotalBytes,
			Shared:         ps.Spec.Shared,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

// volumeByID finds a ProxmoxVolume by status.volumeID in a namespace.
//
//nolint:dupl // Structurally identical to snapshotByID but operates on a different type and index.
func (s *Service) volumeByID(ctx context.Context, namespace, volumeID string) (*v1alpha1.ProxmoxVolume, error) {
	list := &v1alpha1.ProxmoxVolumeList{}
	if err := s.client.List(ctx, list,
		client.InNamespace(namespace),
		client.MatchingFields{VolumeIDIndex: volumeID},
	); err != nil {
		return nil, status.Errorf(codes.Internal, "listing volumes: %v", err)
	}

	if len(list.Items) == 0 {
		return nil, status.Errorf(codes.NotFound, "volume %s not found in namespace %s", volumeID, namespace)
	}

	return &list.Items[0], nil
}

// resolveVMID resolves and validates a VMID for the attach request.
//
// If the request carries a nonzero vmid, it must be in the tenant's
// resolvedVmids. If it is zero, the VMID is derived from the node ID:
// ParseNodeID may yield one directly, otherwise the VM is looked up by name.
// The result is always restricted to the tenant's resolvedVmids.
func (s *Service) resolveVMID(ctx context.Context, tenant *v1alpha1.TenantCluster, nodeID string, reqVMID int32) (int32, error) {
	if reqVMID != 0 {
		if !slices.Contains(tenant.Status.ResolvedVMIDs, reqVMID) {
			return 0, status.Errorf(codes.FailedPrecondition,
				"vmid %d is not in tenant %s's resolvedVmids", reqVMID, tenant.Name)
		}

		return reqVMID, nil
	}

	// Parse the CSI node ID.
	n, err := utilsnode.ParseNodeID(nodeID)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid node_id %q: %v", nodeID, err)
	}

	if id, err := n.GetVMID(); err == nil && id != 0 {
		vmid := int32(id)
		if !slices.Contains(tenant.Status.ResolvedVMIDs, vmid) {
			return 0, status.Errorf(codes.FailedPrecondition,
				"vmid %d (from node ID) is not in tenant %s's resolvedVmids", vmid, tenant.Name)
		}

		return vmid, nil
	}

	// Fallback: match VM name to node name, restricted to resolvedVmids.
	return s.resolveVMIDByName(ctx, tenant, n.GetNodeName())
}

// resolveVMIDByName looks up a VM whose name equals the node name, restricted
// to the tenant's resolvedVmids.
func (s *Service) resolveVMIDByName(ctx context.Context, tenant *v1alpha1.TenantCluster, nodeName string) (int32, error) {
	if s.vms == nil {
		return 0, status.Errorf(codes.FailedPrecondition,
			"cannot resolve node %q to a VMID: no VMID in node ID and VM reader is not available", nodeName)
	}

	allVMs, err := s.vms.ListVMs(ctx, tenant.Spec.Region)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "listing VMs for VMID resolution: %v", err)
	}

	resolved := make(map[int32]struct{}, len(tenant.Status.ResolvedVMIDs))
	for _, id := range tenant.Status.ResolvedVMIDs {
		resolved[id] = struct{}{}
	}

	var matches []int32

	for _, vm := range allVMs {
		if vm.Name != nodeName {
			continue
		}

		if _, ok := resolved[vm.VMID]; ok {
			matches = append(matches, vm.VMID)
		}
	}

	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return 0, status.Errorf(codes.FailedPrecondition,
			"no VM named %q found in tenant %s's resolvedVmids", nodeName, tenant.Name)
	default:
		return 0, status.Errorf(codes.FailedPrecondition,
			"multiple VMs named %q found in tenant %s's resolvedVmids: %v", nodeName, tenant.Name, matches)
	}
}

// attachmentName computes the deterministic ProxmoxVolumeAttachment name:
// "pva-" + first 16 hex characters of sha256(volumeID + "\x00" + nodeID).
func attachmentName(volumeID, nodeID string) string {
	h := sha256.Sum256([]byte(volumeID + "\x00" + nodeID))

	return fmt.Sprintf("pva-%x", h[:8])
}

// lunFromStatus parses the LUN string from the attachment status into an int32.
func lunFromStatus(lun string) int32 {
	if lun == "" {
		return 0
	}

	n, err := strconv.ParseInt(lun, 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}

// IndexVolumeIDs registers the volumeID and snapshotID field indexes.
func IndexVolumeIDs(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &v1alpha1.ProxmoxVolume{}, VolumeIDIndex, IndexVolumeID); err != nil {
		return fmt.Errorf("indexing volume IDs: %w", err)
	}

	if err := indexer.IndexField(ctx, &v1alpha1.ProxmoxVolumeSnapshot{}, SnapshotIDIndex, IndexSnapshotID); err != nil {
		return fmt.Errorf("indexing snapshot IDs: %w", err)
	}

	return nil
}
