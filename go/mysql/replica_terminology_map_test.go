/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import "testing"

func TestReplicaTermForMariaDB11KeepsLegacyTerms(t *testing.T) {
	tests := []string{
		"master status",
		"slave status",
		"Slave_IO_Running",
		"Seconds_Behind_Master",
	}

	version := "11.4.8-MariaDB-ubu2404-log"
	for _, term := range tests {
		if actual := ReplicaTermFor(version, term); actual != term {
			t.Fatalf("term %q: expected MariaDB to keep legacy term %q, got %q", term, term, actual)
		}
	}
}
