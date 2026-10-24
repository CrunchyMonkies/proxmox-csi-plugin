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
	"testing"

	"github.com/stretchr/testify/assert"

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
