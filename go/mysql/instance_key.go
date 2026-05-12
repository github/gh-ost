/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const DefaultInstancePort = 3306

var (
	ipv4HostPortRegexp = regexp.MustCompile("^([^:]+):([0-9]+)$")
	ipv4HostRegexp     = regexp.MustCompile("^([^:]+)$")

	// e.g. [2001:db8:1f70::999:de8:7648:6e8]:3308
	ipv6HostPortRegexp = regexp.MustCompile(`^\[([0-9a-fA-F:]+)\]:(\d{1,5})$`) //nolint:gosimple
	// e.g. 2001:db8:1f70::999:de8:7648:6e8
	ipv6HostRegexp = regexp.MustCompile("^([:0-9a-fA-F]+)$")
)

// InstanceKey is an instance indicator, identified by hostname and port
type InstanceKey struct {
	Hostname string
	Port     int
}

const detachHint = "//"

// ParseInstanceKey will parse an InstanceKey from a string representation such as 127.0.0.1:3306
func NewRawInstanceKey(hostPort string) (*InstanceKey, error) {
	var hostname, port string
	if submatch := ipv4HostPortRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
		port = submatch[2]
	} else if submatch := ipv4HostRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
	} else if submatch := ipv6HostPortRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
		port = submatch[2]
	} else if submatch := ipv6HostRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
	} else {
		return nil, fmt.Errorf("cannot parse address: %s", hostPort)
	}
	instanceKey := &InstanceKey{Hostname: hostname, Port: DefaultInstancePort}
	if port != "" {
		var err error
		if instanceKey.Port, err = strconv.Atoi(port); err != nil {
			return instanceKey, fmt.Errorf("invalid port: %s", port)
		}
	}

	return instanceKey, nil
}

// ParseInstanceKey will parse an InstanceKey from a string representation such as 127.0.0.1:3306.
// The port part is optional; there will be no name resolve
func ParseInstanceKey(hostPort string) (*InstanceKey, error) {
	return NewRawInstanceKey(hostPort)
}

// Equals tests equality between this key and another key
func (ik *InstanceKey) Equals(other *InstanceKey) bool {
	if other == nil {
		return false
	}
	return ik.Hostname == other.Hostname && ik.Port == other.Port
}

// SmallerThan returns true if this key is dictionary-smaller than another.
// This is used for consistent sorting/ordering; there's nothing magical about it.
func (ik *InstanceKey) SmallerThan(other *InstanceKey) bool {
	if ik.Hostname < other.Hostname {
		return true
	}
	if ik.Hostname == other.Hostname && ik.Port < other.Port {
		return true
	}
	return false
}

// IsDetached returns 'true' when this hostname is logically "detached"
func (ik *InstanceKey) IsDetached() bool {
	return strings.HasPrefix(ik.Hostname, detachHint)
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (ik *InstanceKey) IsValid() bool {
	if ik.Hostname == "_" {
		return false
	}
	if ik.IsDetached() {
		return false
	}
	return len(ik.Hostname) > 0 && ik.Port > 0
}

// DetachedKey returns an instance key whose hostname is detached: invalid, but recoverable
func (ik *InstanceKey) DetachedKey() *InstanceKey {
	if ik.IsDetached() {
		return ik
	}
	return &InstanceKey{Hostname: fmt.Sprintf("%s%s", detachHint, ik.Hostname), Port: ik.Port}
}

// ReattachedKey returns an instance key whose hostname is detached: invalid, but recoverable
func (ik *InstanceKey) ReattachedKey() *InstanceKey {
	if !ik.IsDetached() {
		return ik
	}
	return &InstanceKey{Hostname: ik.Hostname[len(detachHint):], Port: ik.Port}
}

// StringCode returns an official string representation of this key
func (ik *InstanceKey) StringCode() string {
	return fmt.Sprintf("%s:%d", ik.Hostname, ik.Port)
}

// DisplayString returns a user-friendly string representation of this key
func (ik *InstanceKey) DisplayString() string {
	return ik.StringCode()
}

// String returns a user-friendly string representation of this key
func (ik InstanceKey) String() string {
	return ik.StringCode()
}
