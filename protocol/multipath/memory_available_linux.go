//go:build linux

package multipath

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func availableMemory() (uint64, error) {
	return availableMemoryWith(os.ReadFile)
}

func availableMemoryWith(readFile func(string) ([]byte, error)) (uint64, error) {
	data, err := readFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "MemAvailable:" {
			continue
		}
		kilobytes, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			return 0, parseErr
		}
		if kilobytes > ^uint64(0)/1024 {
			return 0, errors.New("MemAvailable overflows bytes")
		}
		return cgroupAvailableMemory(readFile, kilobytes*1024), nil
	}
	return 0, errors.New("MemAvailable is missing from /proc/meminfo")
}

// cgroup limits are hierarchical. The process's own group can be unlimited
// while a visible parent has less headroom than /proc/meminfo reports. Usage
// includes other processes in each group; treating it all as occupied is
// deliberately conservative, without assuming file-cache reclaim will succeed.
func cgroupAvailableMemory(readFile func(string) ([]byte, error), available uint64) uint64 {
	membership, err := readFile("/proc/self/cgroup")
	if err != nil {
		return available
	}
	mounts, err := readFile("/proc/self/mountinfo")
	if err != nil {
		return available
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for _, member := range strings.Split(string(membership), "\n") {
		parts := strings.SplitN(member, ":", 3)
		if len(parts) != 3 || !filepath.IsAbs(parts[2]) {
			continue
		}
		v2 := parts[0] == "0" && parts[1] == ""
		if !v2 && !containsController(parts[1], "memory") {
			continue
		}
		for _, line := range strings.Split(string(mounts), "\n") {
			left, right, ok := strings.Cut(line, " - ")
			fields, fs := strings.Fields(left), strings.Fields(right)
			if !ok || len(fields) < 5 || len(fs) < 3 ||
				(v2 && fs[0] != "cgroup2") || (!v2 && (fs[0] != "cgroup" || !containsController(fs[2], "memory"))) {
				continue
			}
			root, mount := filepath.Clean(unescape.Replace(fields[3])), filepath.Clean(unescape.Replace(fields[4]))
			rel, err := filepath.Rel(root, filepath.Clean(parts[2]))
			if err != nil || rel == ".." || strings.HasPrefix(rel, "../") || !filepath.IsAbs(mount) {
				continue
			}
			limitName, usageName := "memory.max", "memory.current"
			if !v2 {
				limitName, usageName = "memory.limit_in_bytes", "memory.usage_in_bytes"
			}
			for dir := filepath.Join(mount, rel); ; dir = filepath.Dir(dir) {
				limit, limitErr := readMemoryCounter(readFile, filepath.Join(dir, limitName))
				used, usageErr := readMemoryCounter(readFile, filepath.Join(dir, usageName))
				if limitErr == nil && usageErr == nil {
					available = min(available, limit-min(limit, used))
				}
				if dir == mount {
					break
				}
			}
		}
	}
	return available
}

func containsController(list, name string) bool {
	return strings.Contains(","+list+",", ","+name+",")
}

func readMemoryCounter(readFile func(string) ([]byte, error), path string) (uint64, error) {
	data, err := readFile(path)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory counter %s: %w", path, err)
	}
	return value, nil
}
