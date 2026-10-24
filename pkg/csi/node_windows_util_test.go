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
)

func TestDiskUniqueIDFromPublishContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ctx     map[string]string
		want    string
		wantErr bool
	}{
		{
			name: "controller device path",
			// hex("PVC-ID05"), as AttachVolume writes it; Windows showed 5056432D49443035.
			ctx:  map[string]string{"DevicePath": "/dev/disk/by-id/wwn-0x5056432d49443035", "lun": "5"},
			want: "5056432D49443035",
		},
		{name: "missing", ctx: map[string]string{}, wantErr: true},
		{name: "not a wwn path", ctx: map[string]string{"DevicePath": "/dev/sdb"}, wantErr: true},
		{name: "empty wwn", ctx: map[string]string{"DevicePath": "/dev/disk/by-id/wwn-0x"}, wantErr: true},
		{name: "not hex", ctx: map[string]string{"DevicePath": "/dev/disk/by-id/wwn-0x50\"; rm"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := diskUniqueIDFromPublishContext(tt.ctx)
			if tt.wantErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWindowsFSType(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "ntfs", "NTFS"} {
		got, err := windowsFSType(in)
		assert.NoError(t, err, in)
		assert.Equal(t, FSTypeNTFS, got)
	}

	for _, in := range []string{FSTypeExt4, FSTypeXfs, "refs"} {
		_, err := windowsFSType(in)
		assert.Error(t, err, in)
	}
}

func TestIsZeroed(t *testing.T) {
	t.Parallel()

	buf := make([]byte, windowsBlankProbeBytes)
	assert.True(t, isZeroed(buf))

	// ext4 superblock magic 0xEF53 at 1024+56.
	buf[1024+56], buf[1024+57] = 0x53, 0xef
	assert.False(t, isZeroed(buf))
}

func TestWindowsAccessPath(t *testing.T) {
	t.Parallel()

	want := `c:\var\lib\kubelet\plugins\kubernetes.io\csi\x\globalmount\`

	assert.Equal(t, want, windowsAccessPath(`c:\var\lib\kubelet\plugins\kubernetes.io\csi\x\globalmount`))
	assert.Equal(t, want, windowsAccessPath(`c:\var\lib\kubelet\plugins\kubernetes.io\csi\x\globalmount\`))
	assert.Equal(t, want, windowsAccessPath(`c:/var/lib/kubelet/plugins/kubernetes.io/csi/x/globalmount`))
}

func TestWindowsUsersWritable(t *testing.T) {
	t.Parallel()

	for in, want := range map[string]bool{"": false, "0755": false, "0700": false, "0775": true, "0777": true, "0757": true} {
		got, err := windowsUsersWritable(in)
		assert.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	_, err := windowsUsersWritable("rwx")
	assert.Error(t, err)
}
