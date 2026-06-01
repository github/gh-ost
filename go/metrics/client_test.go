/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"slices"
	"testing"
)

func TestNewClient_NoAddr_ReturnsNoopSingleton(t *testing.T) {
	c, err := NewClient("", []string{"env:test"}, "gh_ost.")
	if err != nil {
		t.Fatal(err)
	}
	if c != Noop || c.sd != nil {
		t.Fatalf("expected Noop singleton without statsd connection, got %p sd=%v", c, c.sd)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMergeTagSlices(t *testing.T) {
	tests := []struct {
		name    string
		global  []string
		perCall []string
		want    []string
	}{
		{"nil_global", nil, []string{"k:v"}, []string{"k:v"}},
		{"empty_extra", []string{"env:prod"}, nil, []string{"env:prod"}},
		{"combined", []string{"env:prod"}, []string{"shard:1"}, []string{"env:prod", "shard:1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeTagSlices(tt.global, tt.perCall)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("got %#v want %#v", got, tt.want)
			}
		})
	}
}

func mergeTagSlices(global, perCall []string) []string {
	if len(global) == 0 {
		return perCall
	}
	if len(perCall) == 0 {
		return global
	}
	out := make([]string, 0, len(global)+len(perCall))
	return append(append(out, global...), perCall...)
}
