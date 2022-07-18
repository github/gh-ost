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
	ipv6HostPortRegexp = regexp.MustCompile("^\\[([:0-9a-fA-F]+)\\]:([0-9]+)$") //nolint:gosimple
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
		return nil, fmt.Errorf("Cannot parse address: %s", hostPort)
	}
	instanceKey := &InstanceKey{Hostname: hostname, Port: DefaultInstancePort}
	if port != "" {
		var err error
		if instanceKey.Port, err = strconv.Atoi(port); err != nil {
			return instanceKey, fmt.Errorf("Invalid port: %s", port)
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
func (this *InstanceKey) Equals(other *InstanceKey) bool {
	if other == nil {
		return false
	}
	return this.Hostname == other.Hostname && this.Port == other.Port
}

// SmallerThan returns true if this key is dictionary-smaller than another.
// This is used for consistent sorting/ordering; there's nothing magical about it.
func (this *InstanceKey) SmallerThan(other *InstanceKey) bool {
	if this.Hostname < other.Hostname {
		return true
	}
	if this.Hostname == other.Hostname && this.Port < other.Port {
		return true
	}
	return false
}

// IsDetached returns 'true' when this hostname is logically "detached"
func (this *InstanceKey) IsDetached() bool {
	return strings.HasPrefix(this.Hostname, detachHint)
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (this *InstanceKey) IsValid() bool {
	if this.Hostname == "_" {
		return false
	}
	if this.IsDetached() {
		return false
	}
	return len(this.Hostname) > 0 && this.Port > 0
}

// DetachedKey returns an instance key whose hostname is detached: invalid, but recoverable
func (this *InstanceKey) DetachedKey() *InstanceKey {
	if this.IsDetached() {
		return this
	}
	return &InstanceKey{Hostname: fmt.Sprintf("%s%s", detachHint, this.Hostname), Port: this.Port}
}

// ReattachedKey returns an instance key whose hostname is detached: invalid, but recoverable
func (this *InstanceKey) ReattachedKey() *InstanceKey {
	if !this.IsDetached() {
		return this
	}
	return &InstanceKey{Hostname: this.Hostname[len(detachHint):], Port: this.Port}
}

// StringCode returns an official string representation of this key
func (this *InstanceKey) StringCode() string {
	return fmt.Sprintf("%s:%d", this.Hostname, this.Port)
}

// DisplayString returns a user-friendly string representation of this key
func (this *InstanceKey) DisplayString() string {
	return this.StringCode()
}

// String returns a user-friendly string representation of this key
func (this InstanceKey) String() string {
	return this.StringCode()
}
