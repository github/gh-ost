//go:build linux
// +build linux

package statsd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"syscall"
)

const (
	// cgroupPath is the path to the cgroup file where we can find the container id if one exists.
	cgroupPath = "/proc/self/cgroup"

	// selfMountinfo is the path to the mountinfo path where we can find the container id in case cgroup namespace is preventing the use of /proc/self/cgroup
	selfMountInfoPath = "/proc/self/mountinfo"

	// defaultCgroupMountPath is the default path to the cgroup mount point.
	defaultCgroupMountPath = "/sys/fs/cgroup"

	// cgroupV1BaseController is the controller used to identify the container-id for cgroup v1
	cgroupV1BaseController = "memory"

	uuidSource      = "[0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12}"
	containerSource = "[0-9a-f]{64}"
	taskSource      = "[0-9a-f]{32}-\\d+"

	containerdSandboxPrefix = "sandboxes"

	// ContainerRegexpStr defines the regexp used to match container IDs
	// ([0-9a-f]{64}) is standard container id used pretty much everywhere
	// ([0-9a-f]{32}-\d+) is container id used by AWS ECS
	// ([0-9a-f]{8}(-[0-9a-f]{4}){4}$) is container id used by Garden
	containerRegexpStr = "([0-9a-f]{64})|([0-9a-f]{32}-\\d+)|([0-9a-f]{8}(-[0-9a-f]{4}){4}$)"
	// cIDRegexpStr defines the regexp used to match container IDs in /proc/self/mountinfo
	cIDRegexpStr = `.*/([^\s/]+)/(` + containerRegexpStr + `)/[\S]*hostname`

	// From https://github.com/torvalds/linux/blob/5859a2b1991101d6b978f3feb5325dad39421f29/include/linux/proc_ns.h#L41-L49
	// Currently, host namespace inode number are hardcoded, which can be used to detect
	// if we're running in host namespace or not (does not work when running in DinD)
	hostCgroupNamespaceInode = 0xEFFFFFFB
)

var (
	// expLine matches a line in the /proc/self/cgroup file. It has a submatch for the last element (path), which contains the container ID.
	expLine = regexp.MustCompile(`^\d+:[^:]*:(.+)$`)

	// expContainerID matches contained IDs and sources. Source: https://github.com/Qard/container-info/blob/master/index.js
	expContainerID = regexp.MustCompile(fmt.Sprintf(`(%s|%s|%s)(?:.scope)?$`, uuidSource, containerSource, taskSource))

	cIDMountInfoRegexp = regexp.MustCompile(cIDRegexpStr)

	// initContainerID initializes the container ID.
	initContainerID = internalInitContainerID
)

// parseContainerID finds the first container ID reading from r and returns it.
func parseContainerID(r io.Reader) string {
	scn := bufio.NewScanner(r)
	for scn.Scan() {
		path := expLine.FindStringSubmatch(scn.Text())
		if len(path) != 2 {
			// invalid entry, continue
			continue
		}
		if parts := expContainerID.FindStringSubmatch(path[1]); len(parts) == 2 {
			return parts[1]
		}
	}
	return ""
}

// readContainerID attempts to return the container ID from the provided file path or empty on failure.
func readContainerID(fpath string) string {
	f, err := os.Open(fpath)
	if err != nil {
		return ""
	}
	defer f.Close()
	return parseContainerID(f)
}

// Parsing /proc/self/mountinfo is not always reliable in Kubernetes+containerd (at least)
// We're still trying to use it as it may help in some cgroupv2 configurations (Docker, ECS, raw containerd)
func parseMountinfo(r io.Reader) string {
	scn := bufio.NewScanner(r)
	for scn.Scan() {
		line := scn.Text()
		allMatches := cIDMountInfoRegexp.FindAllStringSubmatch(line, -1)
		if len(allMatches) == 0 {
			continue
		}

		// We're interest in rightmost match
		matches := allMatches[len(allMatches)-1]
		if len(matches) > 0 && matches[1] != containerdSandboxPrefix {
			return matches[2]
		}
	}

	return ""
}

func readMountinfo(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	return parseMountinfo(f)
}

func isHostCgroupNamespace() bool {
	fi, err := os.Stat("/proc/self/ns/cgroup")
	if err != nil {
		return false
	}

	inode := fi.Sys().(*syscall.Stat_t).Ino

	return inode == hostCgroupNamespaceInode
}

// parseCgroupNodePath parses /proc/self/cgroup and returns a map of controller to its associated cgroup node path.
func parseCgroupNodePath(r io.Reader) map[string]string {
	res := make(map[string]string)
	scn := bufio.NewScanner(r)
	for scn.Scan() {
		line := scn.Text()
		tokens := strings.Split(line, ":")
		if len(tokens) != 3 {
			continue
		}
		if tokens[1] == cgroupV1BaseController || tokens[1] == "" {
			res[tokens[1]] = tokens[2]
		}
	}
	return res
}

// getCgroupInode returns the cgroup controller inode if it exists otherwise an empty string.
// The inode is prefixed by "in-" and is used by the agent to retrieve the container ID.
// For cgroup v1, we use the memory controller.
func getCgroupInode(cgroupMountPath, procSelfCgroupPath string) string {
	// Parse /proc/self/cgroup to retrieve the paths to the memory controller (cgroupv1) and the cgroup node (cgroupv2)
	f, err := os.Open(procSelfCgroupPath)
	if err != nil {
		return ""
	}
	defer f.Close()
	cgroupControllersPaths := parseCgroupNodePath(f)
	// Retrieve the cgroup inode from /sys/fs/cgroup+controller+cgroupNodePath
	for _, controller := range []string{cgroupV1BaseController, ""} {
		cgroupNodePath, ok := cgroupControllersPaths[controller]
		if !ok {
			continue
		}
		inode := inodeForPath(path.Join(cgroupMountPath, controller, cgroupNodePath))
		if inode != "" {
			return inode
		}
	}
	return ""
}

// inodeForPath returns the inode for the provided path or empty on failure.
func inodeForPath(path string) string {
	fi, err := os.Stat(path)
	if err != nil {
		return ""
	}
	stats, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("in-%d", stats.Ino)
}

// internalInitContainerID initializes the container ID.
// It can either be provided by the user or read from cgroups.
func internalInitContainerID(userProvidedID string, cgroupFallback, isHostCgroupNs bool) {
	initOnce.Do(func() {
		readCIDOrInode(userProvidedID, cgroupPath, selfMountInfoPath, defaultCgroupMountPath, cgroupFallback, isHostCgroupNs)
	})
}

// readCIDOrInode reads the container ID from the user provided ID, cgroups or mountinfo.
func readCIDOrInode(userProvidedID, cgroupPath, selfMountInfoPath, defaultCgroupMountPath string, cgroupFallback, isHostCgroupNs bool) {
	if userProvidedID != "" {
		containerID = userProvidedID
		return
	}

	if cgroupFallback {
		containerID = readContainerID(cgroupPath)
		if containerID != "" {
			return
		}

		containerID = readMountinfo(selfMountInfoPath)
		if containerID != "" {
			return
		}

		// If we're in the host cgroup namespace, the cid should be retrievable in /proc/self/cgroup
		// In private cgroup namespace, we can retrieve the cgroup controller inode.
		if containerID == "" && isHostCgroupNs {
			return
		}

		containerID = getCgroupInode(defaultCgroupMountPath, cgroupPath)
	}
}
