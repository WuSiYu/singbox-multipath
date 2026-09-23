//go:build linux

package multipath

import (
	"os"
	"testing"
)

func TestCgroupAvailableMemory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  uint64
	}{
		{"host", nil, 2 << 30},
		{"v2_leaf", map[string]string{
			"/proc/self/cgroup":                            "0::/services/proxy\n",
			"/proc/self/mountinfo":                         "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
			"/sys/fs/cgroup/services/proxy/memory.max":     "1073741824\n",
			"/sys/fs/cgroup/services/proxy/memory.current": "268435456\n",
		}, 768 << 20},
		{"v2_parent", map[string]string{
			"/proc/self/cgroup":                            "0::/services/proxy\n",
			"/proc/self/mountinfo":                         "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
			"/sys/fs/cgroup/services/proxy/memory.max":     "max\n",
			"/sys/fs/cgroup/services/proxy/memory.current": "100\n",
			"/sys/fs/cgroup/services/memory.max":           "1073741824\n",
			"/sys/fs/cgroup/services/memory.current":       "805306368\n",
		}, 256 << 20},
		{"namespace_root", map[string]string{
			"/proc/self/cgroup":             "0::/\n",
			"/proc/self/mountinfo":          "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
			"/sys/fs/cgroup/memory.max":     "134217728\n",
			"/sys/fs/cgroup/memory.current": "33554432\n",
		}, 96 << 20},
		{"v1_mount_root", map[string]string{
			"/proc/self/cgroup":    "7:cpu:/unused\n8:memory:/tenant/proxy\n",
			"/proc/self/mountinfo": "1 0 0:1 /tenant /sys/fs/cgroup/mem\\040group rw - cgroup cgroup rw,memory\n",
			"/sys/fs/cgroup/mem group/proxy/memory.limit_in_bytes": "1073741824\n",
			"/sys/fs/cgroup/mem group/proxy/memory.usage_in_bytes": "536870912\n",
		}, 512 << 20},
		{"exhausted", map[string]string{
			"/proc/self/cgroup":             "0::/\n",
			"/proc/self/mountinfo":          "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
			"/sys/fs/cgroup/memory.max":     "10\n",
			"/sys/fs/cgroup/memory.current": "20\n",
		}, 0},
		{"unrelated_mount", map[string]string{
			"/proc/self/cgroup":             "0::/other\n",
			"/proc/self/mountinfo":          "1 0 0:1 /tenant /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
			"/sys/fs/cgroup/memory.max":     "10\n",
			"/sys/fs/cgroup/memory.current": "20\n",
		}, 2 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read := func(path string) ([]byte, error) {
				if path == "/proc/meminfo" {
					return []byte("MemAvailable: 2097152 kB\n"), nil
				}
				if data, ok := tc.files[path]; ok {
					return []byte(data), nil
				}
				return nil, os.ErrNotExist
			}
			got, err := availableMemoryWith(read)
			if err != nil || got != tc.want {
				t.Fatalf("got %d %v, want %d", got, err, tc.want)
			}
			if got == 0 && automaticMemoryLimit(got) != 0 {
				t.Fatal("exhausted cgroup must not receive a fallback budget")
			}
		})
	}
}
