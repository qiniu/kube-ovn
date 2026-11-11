//go:build linux

package speaker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cilium/ebpf"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestRemoveBPFPin(t *testing.T) {
	// 1. 删除不存在的文件应安全返回 nil
	require.NoError(t, removeBPFPin("/tmp/nonexistent-pin-file-12345"))

	// 2. 正常删除已有文件
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test-pin")
	require.NoError(t, os.WriteFile(filePath, []byte("test"), 0o600))
	require.FileExists(t, filePath)
	require.NoError(t, removeBPFPin(filePath))
	require.NoFileExists(t, filePath)
}

func TestLoadCollectionWithPinnedMaps(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		name := "fresh"
		if fallback {
			name = "incompatible"
		}
		t.Run(name, func(t *testing.T) {
			path := testTCPinPath(t)
			if fallback {
				old, err := ebpf.NewMap(&ebpf.MapSpec{Type: ebpf.Hash, KeySize: 4, ValueSize: 8, MaxEntries: 1})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, old.Close()) })
				require.NoError(t, old.Pin(path))
			}
			spec := &ebpf.CollectionSpec{Maps: map[string]*ebpf.MapSpec{
				"test_map": {Name: "test_map", Type: ebpf.Hash, KeySize: 4, ValueSize: 4, MaxEntries: 1},
			}}
			coll, err := loadCollectionWithPinnedMaps(spec, map[string]string{"test_map": path})
			require.NoError(t, err)
			t.Cleanup(coll.Close)
			require.NoError(t, coll.Maps["test_map"].Put(uint32(1), uint32(42)))
			pinned, err := ebpf.LoadPinnedMap(path, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pinned.Close()) })
			var value uint32
			require.NoError(t, pinned.Lookup(uint32(1), &value))
			require.Equal(t, uint32(42), value)
			entries, err := os.ReadDir(filepath.Dir(path))
			require.NoError(t, err)
			require.Len(t, entries, 1, "temporary pins must not remain after commit")
		})
	}
}

func TestPruneBPFHashMapNil(t *testing.T) {
	require.ErrorContains(t, pruneBPFHashMap(nil, map[string]bool{}), "target map is nil")
}

func TestLoadCollectionWithPinnedMapsMissingMap(t *testing.T) {
	if os.Getenv("KUBE_OVN_BPF_TEST") != "1" {
		t.Skip("requires privileged BPF environment")
	}
	if err := unix.Mount("bpf", "/sys/fs/bpf", "bpf", 0, ""); err != nil {
		require.ErrorIs(t, err, unix.EBUSY)
	}
	dir, err := os.MkdirTemp("/sys/fs/bpf", "bpf-missing-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })

	spec := &ebpf.CollectionSpec{
		Maps: map[string]*ebpf.MapSpec{
			"valid_map": {
				Type:       ebpf.Hash,
				KeySize:    4,
				ValueSize:  4,
				MaxEntries: 10,
			},
		},
	}
	pinPaths := map[string]string{
		"missing_map": filepath.Join(dir, "missing_map"),
	}
	coll, err := loadCollectionWithPinnedMaps(spec, pinPaths)
	require.ErrorContains(t, err, "missing_map not found in collection")
	require.Nil(t, coll)
}
