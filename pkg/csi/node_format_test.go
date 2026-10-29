//go:build !windows

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

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/helpers/ptr"
)

func TestCollectFormatOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg      string
		params   StorageParameters
		fsType   string
		expected []string
	}{
		{
			msg:      "ext4 skips discard and journal zeroing at mkfs",
			fsType:   FSTypeExt4,
			expected: []string{"-E", "nodiscard,lazy_itable_init=1,lazy_journal_init=1"},
		},
		{
			msg:      "ext3 skips discard and journal zeroing at mkfs",
			fsType:   "ext3",
			expected: []string{"-E", "nodiscard,lazy_itable_init=1,lazy_journal_init=1"},
		},
		{
			msg:      "xfs never discards at mkfs",
			fsType:   FSTypeXfs,
			expected: []string{"-K"},
		},
		{
			msg:      "unknown filesystem gets no options",
			fsType:   "btrfs",
			expected: []string{},
		},
		{
			msg:      "ext4 keeps block and inode size",
			params:   StorageParameters{BlockSize: ptr.Ptr(4096), InodeSize: ptr.Ptr(256)},
			fsType:   FSTypeExt4,
			expected: []string{"-E", "nodiscard,lazy_itable_init=1,lazy_journal_init=1", "-b", "4096", "-I", "256"},
		},
		{
			msg:      "xfs keeps block and inode size in its own syntax",
			params:   StorageParameters{BlockSize: ptr.Ptr(4096), InodeSize: ptr.Ptr(512)},
			fsType:   FSTypeXfs,
			expected: []string{"-K", "-b", "size=4096", "-i", "size=512"},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.msg, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.expected, collectFormatOptions(testCase.params, testCase.fsType))
		})
	}
}

func TestFindVirtIODevice(t *testing.T) {
	t.Parallel()

	sysBlock := t.TempDir()

	for dev, serial := range map[string]string{
		"vda": "",
		"vdb": "PVC-ID31\n",
		"vdc": "PVC-ID32\n",
		"sda": "PVC-ID33\n",
	} {
		require.NoError(t, os.MkdirAll(filepath.Join(sysBlock, dev), 0o755))

		if serial != "" {
			require.NoError(t, os.WriteFile(filepath.Join(sysBlock, dev, "serial"), []byte(serial), 0o644))
		}
	}

	assert.Equal(t, "/dev/vdc", findVirtIODevice(sysBlock, "PVC-ID32"))
	assert.Equal(t, "/dev/vdb", findVirtIODevice(sysBlock, "PVC-ID31"))
	// Only virtio-blk disks are considered.
	assert.Empty(t, findVirtIODevice(sysBlock, "PVC-ID33"))
	assert.Empty(t, findVirtIODevice(sysBlock, "PVC-ID40"))
	assert.Empty(t, findVirtIODevice(filepath.Join(sysBlock, "missing"), "PVC-ID31"))
}

func TestMarkNonRotational(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	sysBlock := filepath.Join(root, "sys")
	devDir := filepath.Join(root, "dev")

	for _, dev := range []string{"vdb", "sda"} {
		require.NoError(t, os.MkdirAll(filepath.Join(sysBlock, dev, "queue"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(sysBlock, dev, "queue", "rotational"), []byte("1\n"), 0o644))
	}

	require.NoError(t, os.MkdirAll(filepath.Join(devDir, "disk", "by-id"), 0o755))

	for _, dev := range []string{"vdb", "sda"} {
		require.NoError(t, os.WriteFile(filepath.Join(devDir, dev), nil, 0o644))
	}

	// The by-id link is resolved to the disk it points at.
	link := filepath.Join(devDir, "disk", "by-id", "virtio-PVC-ID31")
	require.NoError(t, os.Symlink(filepath.Join(devDir, "vdb"), link))

	require.NoError(t, markNonRotational(sysBlock, link))
	require.NoError(t, markNonRotational(sysBlock, filepath.Join(devDir, "sda")))

	b, err := os.ReadFile(filepath.Join(sysBlock, "vdb", "queue", "rotational"))
	require.NoError(t, err)
	assert.Equal(t, "0", string(b))

	// SCSI disks already carry PVE's rotation rate and are left alone.
	b, err = os.ReadFile(filepath.Join(sysBlock, "sda", "queue", "rotational"))
	require.NoError(t, err)
	assert.Equal(t, "1\n", string(b))

	assert.Error(t, markNonRotational(sysBlock, filepath.Join(devDir, "missing")))
}
