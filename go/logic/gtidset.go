/*
	Copyright 2019 Zach Moazeni.
	See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"strings"
)

type GTIDSet struct {
	server_uuid   string
	gtid_executed string
}

func NewGTIDSet(server_uuid, gtid_executed string) GTIDSet {
	return GTIDSet{server_uuid, gtid_executed}
}

func (set *GTIDSet) originalServerGTIDs() (string, bool) {
	if !strings.Contains(set.gtid_executed, set.server_uuid) {
		return "", false
	}
	gtids := strings.Split(set.gtid_executed, ",")
	for _, gtid := range gtids {
		if strings.Contains(gtid, set.server_uuid) {
			return strings.TrimSpace(gtid), true
		}
	}

	// Shouldn't run into this. Only necessary for type checker
	return "", false
}

func (set *GTIDSet) Revert(current_gtid_executed string) string {
	if !strings.Contains(current_gtid_executed, set.server_uuid) {
		return current_gtid_executed
	}
	gtids := strings.Split(current_gtid_executed, ",")
	newGtids := make([]string, 0, len(gtids))
	originalServerGTIDs, found := set.originalServerGTIDs()
	if found {
		// revert back to original gtid_executed JUST for the server_uuid
		for _, gtid := range gtids {
			if strings.Contains(gtid, set.server_uuid) {
				newGtids = append(newGtids, originalServerGTIDs)
			} else {
				newGtids = append(newGtids, strings.TrimSpace(gtid))
			}
		}
	} else {
		// trim server_uuid current_gtid_executed out of the set and return
		for _, gtid := range gtids {
			if !strings.Contains(gtid, set.server_uuid) {
				newGtids = append(newGtids, strings.TrimSpace(gtid))
			}
		}
	}
	return strings.Join(newGtids, ",\n")
}
