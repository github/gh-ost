//go:build !linux
// +build !linux

package statsd

func isHostCgroupNamespace() bool {
	return false
}

var initContainerID = func(userProvidedID string, _, _ bool) {
	initOnce.Do(func() {
		if userProvidedID != "" {
			containerID = userProvidedID
			return
		}
	})
}
