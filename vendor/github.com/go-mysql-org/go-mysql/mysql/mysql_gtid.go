package mysql

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
)

// Note that MySQL normalized the value set by `SET GTID_NEXT='AUTOMATIC:<tag>`
// by:
// - Removing any length of leading and trailing whitespace (tabs, spaces).
// - Lowercasing the tag
var tagRegexp = regexp.MustCompile(`^\s*[a-zA-Z_][a-zA-Z0-9_]{0,31}\s*$`)

// Normalized tags should match: `^[a-z_][a-z0-9_]{0,31}$`

// Tag is a GTID Tag
type Tag struct {
	normalized string
}

// This ensures that Tag implements the encoding.BinaryMarshaler and Stringer interface
var (
	_ encoding.BinaryMarshaler = Tag{}
	_ fmt.Stringer             = Tag{}
)

func (t Tag) MarshalBinary() ([]byte, error) {
	if len(t.normalized) > 32 {
		return nil, errors.New("tag length too long")
	}
	tagLen := uint8(len(t.normalized) << 1)
	return append([]byte{tagLen}, []byte(t.normalized)...), nil
}

func (t Tag) String() string {
	return t.normalized
}

// NewTag is taking a string and removes leading and trailing whitespace and changes the case to lowercase
func NewTag(str string) Tag {
	if str == "" {
		return Tag{}
	}

	return Tag{strings.TrimSpace(strings.ToLower(str))}
}

// MysqlGTIDSet is storing a map of SIDs (UUIDs), each with one or more tags.
// And each tag has one or more Intervals.
//
//nolint:revive // Can't use mysql.GTIDSet instead of mysql.MysqlGTIDSet as the former already exists and this is a specific implementation of that.
type MysqlGTIDSet map[uuid.UUID]map[Tag]IntervalSlice

// This ensures that MysqlGTIDSet implements the GTIDSet interface
var _ GTIDSet = &MysqlGTIDSet{}

func NewMysqlGTIDSet() MysqlGTIDSet {
	return make(map[uuid.UUID]map[Tag]IntervalSlice)
}

func DecodeMysqlGTIDSet(data []byte) (*MysqlGTIDSet, error) {
	s := NewMysqlGTIDSet()
	format, n, err := DecodeSid(data)
	if err != nil {
		return nil, err
	}
	tag := Tag{}
	pos := 8
	for range n {
		if len(data) < pos+16 {
			return nil, errors.Errorf("invalid gtid set buffer, expected %d or more but got %d", pos+16, len(data))
		}
		sid, err := uuid.FromBytes(data[pos : pos+16])
		if err != nil {
			// This can't happen as uuid.FromBytes() only returns an error if the buffer is less than 16 bytes
			// and we already check for that.
			return nil, err
		}
		pos += 16

		if format == GtidFormatTagged {
			if pos >= len(data) {
				return nil, errors.New("invalid gtid set buffer, tag length expected")
			}
			tagLen := int(data[pos] >> 1)
			pos++

			if pos+tagLen > len(data) {
				return nil, errors.New("invalid gtid set buffer, tag extends beyond data")
			}
			tag = NewTag(string(data[pos : pos+tagLen]))
			pos += tagLen
		}

		if len(data) < pos+8 {
			return nil, errors.Errorf("invalid gtid set buffer, expected %d or more but got %d", pos+8, len(data))
		}
		intervalCount := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		if intervalCount == 0 {
			return nil, errors.New("invalid gtid set buffer, got zero interval count")
		}
		if intervalCount > math.MaxInt/16 { // 16 = minimum interval size of start+stop (8+8)
			return nil, errors.Errorf("invalid gtid set buffer, too many intervals: %d", intervalCount)
		}
		if len(data) < pos+(int(intervalCount)*16) {
			return nil, errors.Errorf("invalid gtid set buffer, expected %d or more but got %d", pos+(int(intervalCount)*16), len(data))
		}
		intervals := make(IntervalSlice, 0, intervalCount)
		for range intervalCount {
			start := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			stop := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8

			intervals = append(intervals, Interval{Start: start, Stop: stop})
		}
		if _, ok := s[sid]; ok {
			s[sid][tag] = append(s[sid][tag], intervals...)
		} else {
			s[sid] = map[Tag]IntervalSlice{
				tag: intervals,
			}
		}
	}

	for sid := range s {
		for tag := range s[sid] {
			s[sid][tag] = s[sid][tag].Normalize()
		}
	}

	if pos < len(data) {
		return &s, errors.Errorf("invalid gtid set buffer, found %d trailing bytes", len(data)-pos)
	}
	return &s, nil
}

func ParseMysqlGTIDSet(str string) (GTIDSet, error) {
	s := NewMysqlGTIDSet()
	if str == "" {
		return &s, nil
	}

	// Each sp has single UUID/SID, but might have multiple sets, each with a unique tag
	sp := strings.SplitSeq(str, ",")

	for part := range sp {
		// Handle UUID/SID
		sep := strings.Split(strings.TrimSpace(part), ":")
		if len(sep) < 2 {
			return nil, errors.Errorf("invalid GTID format, must UUID[:tag]:interval[[:tag]:interval]")
		}

		u, err := uuid.Parse(sep[0])
		if err != nil {
			return nil, errors.Trace(err)
		}

		if _, ok := s[u]; !ok {
			s[u] = make(map[Tag]IntervalSlice, 1)
		}
		// Handle interval(s) and tags
		tag := Tag{}
		for i := 1; i < len(sep); i++ {
			if tagRegexp.MatchString(sep[i]) {
				tag = NewTag(sep[i])
				if _, ok := s[u][tag]; !ok {
					s[u][tag] = nil
				}
			} else {
				in, err := parseInterval(sep[i])
				if err != nil {
					return nil, errors.Trace(err)
				}
				s[u][tag] = append(s[u][tag], in)
			}
		}
		for tag, val := range s[u] {
			if val == nil {
				return nil, errors.Errorf("invalid GTID format, missing interval for tag %s", tag)
			}
			s[u][tag] = val.Normalize()
		}
	}
	return &s, nil
}

func (s *MysqlGTIDSet) AddGTID(uuid uuid.UUID, gno int64) {
	s.AddGTIDWithTag(uuid, Tag{}, gno)
}

func (s *MysqlGTIDSet) AddGTIDWithTag(uuid uuid.UUID, tag Tag, gno int64) {
	_, ok := (*s)[uuid]
	if ok {
		(*s)[uuid][tag] = append((*s)[uuid][tag], Interval{gno, gno + 1}).Normalize()
	} else {
		(*s)[uuid] = map[Tag]IntervalSlice{
			tag: {
				Interval{gno, gno + 1},
			},
		}
	}
}

func (s *MysqlGTIDSet) Clone() GTIDSet {
	g := make(MysqlGTIDSet, len(*s))
	for k, v := range *s {
		newInnerMap := make(map[Tag]IntervalSlice, len(v))
		for k2, v2 := range v {
			newInnerMap[k2] = slices.Clone(v2)
		}
		g[k] = newInnerMap
	}
	return &g
}

func (s *MysqlGTIDSet) Contain(o GTIDSet) bool {
	om, ok := o.(*MysqlGTIDSet)
	if !ok {
		return false
	}
	for k := range *om {
		if _, ok := (*s)[k]; !ok {
			return false
		}
		for k2 := range (*om)[k] {
			i, ok := (*s)[k][k2]
			if !ok {
				return false
			}
			if !i.Contain((*om)[k][k2]) {
				return false
			}
		}
	}
	return true
}

func (s *MysqlGTIDSet) Encode() []byte {
	var buf bytes.Buffer

	format := GtidFormatClassic
	sidCount := uint64(0)
	uuids := make([]uuid.UUID, 0, len(*s))
	for uuid := range *s {
		uuids = append(uuids, uuid)
		for tag := range (*s)[uuid] {
			sidCount++
			if format != GtidFormatTagged && (tag != Tag{}) {
				format = GtidFormatTagged
			}
		}
	}

	sid := encodeSid(format, sidCount)
	buf.Write(sid)

	slices.SortFunc(uuids, func(a, b uuid.UUID) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, uuid := range uuids {
		tags := slices.Collect(maps.Keys((*s)[uuid]))
		slices.SortFunc(tags, func(a, b Tag) int { return strings.Compare(a.normalized, b.normalized) })

		for _, tag := range tags {
			ubin, err := uuid.MarshalBinary()
			if err != nil {
				// should never happen
				slog.Warn("encoding uuid failed", "error", err)
			}
			buf.Write(ubin)

			if format == GtidFormatTagged {
				tbin, err := tag.MarshalBinary()
				if err != nil {
					slog.Warn("encoding tag failed", "error", err)
				}
				buf.Write(tbin)
			}

			_ = binary.Write(&buf, binary.LittleEndian, uint64(len((*s)[uuid][tag])))
			for _, interval := range (*s)[uuid][tag] {
				_ = binary.Write(&buf, binary.LittleEndian, interval.Start)
				_ = binary.Write(&buf, binary.LittleEndian, interval.Stop)
			}
		}
	}

	return buf.Bytes()
}

func (s *MysqlGTIDSet) Equal(o GTIDSet) bool {
	om, ok := o.(*MysqlGTIDSet)
	if !ok {
		return false
	}
	if len(*s) != len(*om) {
		return false
	}
	for u, sm := range *s {
		omm, ok := (*om)[u]
		if !ok || len(sm) != len(omm) {
			return false
		}
		for k, i := range sm {
			if !i.Equal(omm[k]) {
				return false
			}
		}
	}
	return true
}

func (s *MysqlGTIDSet) IsEmpty() bool {
	return len(*s) == 0
}

func (s *MysqlGTIDSet) String() string {
	var sb strings.Builder
	sep := ""
	uuids := make([]uuid.UUID, 0, len(*s))
	for uuid := range *s {
		uuids = append(uuids, uuid)
	}
	slices.SortFunc(uuids, func(a, b uuid.UUID) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, uuid := range uuids {
		sb.WriteString(sep)
		sb.WriteString(uuid.String())
		sep = ","
		tags := make([]Tag, 0, len((*s)[uuid]))
		for tag := range (*s)[uuid] {
			tags = append(tags, tag)
		}
		// Tags are sorted, empty tag first
		slices.SortFunc(tags, func(a, b Tag) int { return strings.Compare(a.normalized, b.normalized) })
		for _, tag := range tags {
			if tag != (Tag{}) {
				sb.WriteString(":")
				sb.WriteString(tag.String())
			}
			for _, interval := range (*s)[uuid][tag] {
				sb.WriteString(":")
				sb.WriteString(interval.String())
			}
		}
	}
	return sb.String()
}

func (s *MysqlGTIDSet) Update(GTIDStr string) error {
	o, err := ParseMysqlGTIDSet(GTIDStr)
	if err != nil {
		return err
	}
	om, ok := o.(*MysqlGTIDSet)
	if !ok {
		// This can't happen as ParseMysqlGTIDSet() always returns a MysqlGTIDSet
		return errors.New("incompatible GTID types")
	}
	for k, v := range *om {
		if _, ok := (*s)[k]; ok {
			for k2, v2 := range (*om)[k] {
				if _, ok := (*s)[k][k2]; ok {
					(*s)[k][k2] = append((*s)[k][k2], (*om)[k][k2]...).Normalize()
				} else {
					(*s)[k][k2] = v2
				}
			}
		} else {
			(*s)[k] = v
		}
	}
	return nil
}

// DecodeSid the number of sids (source identifiers) and if it is using
// tagged GTIDs or classic (non-tagged) GTIDs.
//
// Note that each gtid tag increases the sidnr here, so a single UUID
// might turn up multiple times if there are multiple tags.
//
// see also:
// decode_nsids_format in mysql/mysql-server
// https://github.com/mysql/mysql-server/blob/61a3a1d8ef15512396b4c2af46e922a19bf2b174/sql/rpl_gtid_set.cc#L1363-L1378
func DecodeSid(data []byte) (format GtidFormat, sidnr uint64, err error) {
	if len(data) < 8 {
		return format, 0, errors.New("failed to decode source identifier, input too short")
	}
	if data[7] == 0x1 {
		format = GtidFormatTagged
	}

	if format == GtidFormatTagged {
		masked := make([]byte, 8)
		copy(masked, data[1:7])
		sidnr = binary.LittleEndian.Uint64(masked)
		return format, sidnr, nil
	}
	sidnr = binary.LittleEndian.Uint64(data[:8])
	return format, sidnr, nil
}

func encodeSid(format GtidFormat, sidnr uint64) []byte {
	sid := make([]byte, 8)
	if format == GtidFormatClassic {
		_, _ = binary.Encode(sid, binary.LittleEndian, sidnr)
		return sid
	}
	_, _ = binary.Encode(sid, binary.LittleEndian, sidnr<<8)

	sid[0] = 0x01
	sid[7] = 0x01 // Format marker
	return sid
}

func (f GtidFormat) String() string {
	switch f {
	case GtidFormatClassic:
		return "GtidFormatClassic"
	case GtidFormatTagged:
		return "GtidFormatTagged"
	}
	return fmt.Sprintf("GtidFormat{%d}", int(f))
}
