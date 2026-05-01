/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"encoding/json"
	"strings"
)

// InstanceKeyMap is a convenience struct for listing InstanceKey-s
type InstanceKeyMap map[InstanceKey]bool

func NewInstanceKeyMap() *InstanceKeyMap {
	return &InstanceKeyMap{}
}

func (ikm *InstanceKeyMap) Len() int {
	return len(*ikm)
}

// AddKey adds a single key to this map
func (ikm *InstanceKeyMap) AddKey(key InstanceKey) {
	(*ikm)[key] = true
}

// AddKeys adds all given keys to this map
func (ikm *InstanceKeyMap) AddKeys(keys []InstanceKey) {
	for _, key := range keys {
		ikm.AddKey(key)
	}
}

// HasKey checks if given key is within the map
func (ikm *InstanceKeyMap) HasKey(key InstanceKey) bool {
	_, ok := (*ikm)[key]
	return ok
}

// GetInstanceKeys returns keys in this map in the form of an array
func (ikm *InstanceKeyMap) GetInstanceKeys() []InstanceKey {
	res := []InstanceKey{}
	for key := range *ikm {
		res = append(res, key)
	}
	return res
}

// MarshalJSON will marshal this map as JSON
func (ikm *InstanceKeyMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(ikm.GetInstanceKeys())
}

// ToJSON will marshal this map as JSON
func (ikm *InstanceKeyMap) ToJSON() (string, error) {
	bytes, err := ikm.MarshalJSON()
	return string(bytes), err
}

// ToJSONString will marshal this map as JSON
func (ikm *InstanceKeyMap) ToJSONString() string {
	s, _ := ikm.ToJSON()
	return s
}

// ToCommaDelimitedList will export this map in comma delimited format
func (ikm *InstanceKeyMap) ToCommaDelimitedList() string {
	keyDisplays := []string{}
	for key := range *ikm {
		keyDisplays = append(keyDisplays, key.DisplayString())
	}
	return strings.Join(keyDisplays, ",")
}

// ReadJson unmarshalls a json into this map
func (ikm *InstanceKeyMap) ReadJson(jsonString string) error {
	var keys []InstanceKey
	err := json.Unmarshal([]byte(jsonString), &keys)
	if err != nil {
		return err
	}
	ikm.AddKeys(keys)
	return err
}

// ReadJson unmarshalls a json into this map
func (ikm *InstanceKeyMap) ReadCommaDelimitedList(list string) error {
	if list == "" {
		return nil
	}
	tokens := strings.Split(list, ",")
	for _, token := range tokens {
		key, err := ParseInstanceKey(token)
		if err != nil {
			return err
		}
		ikm.AddKey(*key)
	}
	return nil
}
