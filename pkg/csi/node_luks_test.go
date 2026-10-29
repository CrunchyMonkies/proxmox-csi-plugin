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
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

// fakeCryptsetup records the args and stdin of each cryptsetup call; results holds
// the error returned by each successive call.
func fakeCryptsetup(t *testing.T, calls *[][]string, stdin *[]string, results ...error) *testingexec.FakeExec {
	t.Helper()

	fe := &testingexec.FakeExec{}

	for _, res := range results {
		fe.CommandScript = append(fe.CommandScript, func(cmd string, args ...string) exec.Cmd {
			assert.Equal(t, "cryptsetup", cmd)

			*calls = append(*calls, args)

			fc := &testingexec.FakeCmd{}
			fc.CombinedOutputScript = []testingexec.FakeAction{
				func() ([]byte, []byte, error) {
					in, _ := io.ReadAll(fc.Stdin) //nolint:errcheck
					*stdin = append(*stdin, string(in))

					if res != nil {
						return []byte("refresh failed"), nil, res
					}

					return nil, nil, nil
				},
			}

			return testingexec.InitFakeCmd(fc, cmd, args...)
		})
	}

	return fe
}

func TestLuksAllowDiscards(t *testing.T) {
	t.Parallel()

	exitErr := errors.New("exit 1")
	keyFile := "--key-file=-"
	refresh := []string{"refresh", "sdb-encrypted", "--allow-discards"}
	persistent := append(slices.Clone(refresh), "--persistent", keyFile)
	live := append(slices.Clone(refresh), keyFile)

	for _, tc := range []struct {
		name    string
		results []error
		calls   [][]string
		wantErr bool
	}{
		{
			name:    "luks2 stores the flag in the header",
			results: []error{nil},
			calls:   [][]string{persistent},
		},
		{
			name:    "luks1 falls back to the live mapping",
			results: []error{exitErr, nil},
			calls:   [][]string{persistent, live},
		},
		{
			name:    "both fail",
			results: []error{exitErr, exitErr},
			calls:   [][]string{persistent, live},
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				calls [][]string
				stdin []string
			)

			err := luksAllowDiscards(fakeCryptsetup(t, &calls, &stdin, tc.results...), "sdb-encrypted", []byte("secret"))
			if tc.wantErr {
				assert.ErrorContains(t, err, "refresh failed")
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.calls, calls)

			for _, in := range stdin {
				assert.Equal(t, "secret", in)
			}
		})
	}
}
