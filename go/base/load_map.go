/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// LoadMap is a mapping of status variable & threshold
// e.g. [Threads_connected: 100, Threads_running: 50]
type LoadMap map[string]int64

func NewLoadMap() LoadMap {
	result := make(map[string]int64)
	return result
}

// NewLoadMap parses a `--*-load` flag (e.g. `--max-load`), which is in multiple
// key-value format, such as:
//   'Threads_running=100,Threads_connected=500'
func ParseLoadMap(loadList string) (LoadMap, error) {
	result := NewLoadMap()
	if loadList == "" {
		return result, nil
	}

	loadConditions := strings.Split(loadList, ",")
	for _, loadCondition := range loadConditions {
		loadTokens := strings.Split(loadCondition, "=")
		if len(loadTokens) != 2 {
			return result, fmt.Errorf("Error parsing load condition: %s", loadCondition)
		}
		if loadTokens[0] == "" {
			return result, fmt.Errorf("Error parsing status variable in load condition: %s", loadCondition)
		}
		if n, err := strconv.ParseInt(loadTokens[1], 10, 0); err != nil {
			return result, fmt.Errorf("Error parsing numeric value in load condition: %s", loadCondition)
		} else {
			result[loadTokens[0]] = n
		}
	}

	return result, nil
}

// Duplicate creates a clone of this map
func (this *LoadMap) Duplicate() LoadMap {
	dup := make(map[string]int64)
	for k, v := range *this {
		dup[k] = v
	}
	return dup
}

// String() returns a string representation of this map
func (this *LoadMap) String() string {
	tokens := []string{}
	for key, val := range *this {
		token := fmt.Sprintf("%s=%d", key, val)
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	return strings.Join(tokens, ",")
}
