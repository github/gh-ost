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

func (this *InstanceKeyMap) Len() int {
	return len(*this)
}

// AddKey adds a single key to this map
func (this *InstanceKeyMap) AddKey(key InstanceKey) {
	(*this)[key] = true
}

// AddKeys adds all given keys to this map
func (this *InstanceKeyMap) AddKeys(keys []InstanceKey) {
	for _, key := range keys {
		this.AddKey(key)
	}
}

// HasKey checks if given key is within the map
func (this *InstanceKeyMap) HasKey(key InstanceKey) bool {
	_, ok := (*this)[key]
	return ok
}

// GetInstanceKeys returns keys in this map in the form of an array
func (this *InstanceKeyMap) GetInstanceKeys() []InstanceKey {
	res := []InstanceKey{}
	for key := range *this {
		res = append(res, key)
	}
	return res
}

// MarshalJSON will marshal this map as JSON
func (this *InstanceKeyMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.GetInstanceKeys())
}

// ToJSON will marshal this map as JSON
func (this *InstanceKeyMap) ToJSON() (string, error) {
	bytes, err := this.MarshalJSON()
	return string(bytes), err
}

// ToJSONString will marshal this map as JSON
func (this *InstanceKeyMap) ToJSONString() string {
	s, _ := this.ToJSON()
	return s
}

// ToCommaDelimitedList will export this map in comma delimited format
func (this *InstanceKeyMap) ToCommaDelimitedList() string {
	keyDisplays := []string{}
	for key := range *this {
		keyDisplays = append(keyDisplays, key.DisplayString())
	}
	return strings.Join(keyDisplays, ",")
}

// ReadJson unmarshalls a json into this map
func (this *InstanceKeyMap) ReadJson(jsonString string) error {
	var keys []InstanceKey
	err := json.Unmarshal([]byte(jsonString), &keys)
	if err != nil {
		return err
	}
	this.AddKeys(keys)
	return err
}

// ReadJson unmarshalls a json into this map
func (this *InstanceKeyMap) ReadCommaDelimitedList(list string) error {
	if list == "" {
		return nil
	}
	tokens := strings.Split(list, ",")
	for _, token := range tokens {
		key, err := ParseRawInstanceKeyLoose(token)
		if err != nil {
			return err
		}
		this.AddKey(*key)
	}
	return nil
}
