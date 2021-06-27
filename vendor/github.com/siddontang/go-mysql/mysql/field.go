package mysql

import (
	"encoding/binary"

	"github.com/siddontang/go-mysql/utils"
)

type FieldData []byte

type Field struct {
	Data         FieldData
	Schema       []byte
	Table        []byte
	OrgTable     []byte
	Name         []byte
	OrgName      []byte
	Charset      uint16
	ColumnLength uint32
	Type         uint8
	Flag         uint16
	Decimal      uint8

	DefaultValueLength uint64
	DefaultValue       []byte
}

type FieldValueType uint8

type FieldValue struct {
	Type  FieldValueType
	value uint64 // Also for int64 and float64
	str   []byte
}

const (
	FieldValueTypeNull = iota
	FieldValueTypeUnsigned
	FieldValueTypeSigned
	FieldValueTypeFloat
	FieldValueTypeString
)

func (f *Field) Parse(p FieldData) (err error) {
	f.Data = p

	var n int
	pos := 0
	//skip catelog, always def
	n, err = SkipLengthEncodedString(p)
	if err != nil {
		return
	}
	pos += n

	//schema
	f.Schema, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//table
	f.Table, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//org_table
	f.OrgTable, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//name
	f.Name, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//org_name
	f.OrgName, _, n, err = LengthEncodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//skip oc
	pos += 1

	//charset
	f.Charset = binary.LittleEndian.Uint16(p[pos:])
	pos += 2

	//column length
	f.ColumnLength = binary.LittleEndian.Uint32(p[pos:])
	pos += 4

	//type
	f.Type = p[pos]
	pos++

	//flag
	f.Flag = binary.LittleEndian.Uint16(p[pos:])
	pos += 2

	//decimals 1
	f.Decimal = p[pos]
	pos++

	//filter [0x00][0x00]
	pos += 2

	f.DefaultValue = nil
	//if more data, command was field list
	if len(p) > pos {
		//length of default value lenenc-int
		f.DefaultValueLength, _, n = LengthEncodedInt(p[pos:])
		pos += n

		if pos+int(f.DefaultValueLength) > len(p) {
			err = ErrMalformPacket
			return
		}

		//default value string[$len]
		f.DefaultValue = p[pos:(pos + int(f.DefaultValueLength))]
	}

	return
}

func (p FieldData) Parse() (f *Field, err error) {
	f = new(Field)
	if err = f.Parse(p); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *Field) Dump() []byte {
	if f == nil {
		f = &Field{}
	}
	if f.Data != nil {
		return []byte(f.Data)
	}

	l := len(f.Schema) + len(f.Table) + len(f.OrgTable) + len(f.Name) + len(f.OrgName) + len(f.DefaultValue) + 48

	data := make([]byte, 0, l)

	data = append(data, PutLengthEncodedString([]byte("def"))...)

	data = append(data, PutLengthEncodedString(f.Schema)...)

	data = append(data, PutLengthEncodedString(f.Table)...)
	data = append(data, PutLengthEncodedString(f.OrgTable)...)

	data = append(data, PutLengthEncodedString(f.Name)...)
	data = append(data, PutLengthEncodedString(f.OrgName)...)

	data = append(data, 0x0c)

	data = append(data, Uint16ToBytes(f.Charset)...)
	data = append(data, Uint32ToBytes(f.ColumnLength)...)
	data = append(data, f.Type)
	data = append(data, Uint16ToBytes(f.Flag)...)
	data = append(data, f.Decimal)
	data = append(data, 0, 0)

	if f.DefaultValue != nil {
		data = append(data, Uint64ToBytes(f.DefaultValueLength)...)
		data = append(data, f.DefaultValue...)
	}

	return data
}

func (fv *FieldValue) AsUint64() uint64 {
	return fv.value
}

func (fv *FieldValue) AsInt64() int64 {
	return utils.Uint64ToInt64(fv.value)
}

func (fv *FieldValue) AsFloat64() float64 {
	return utils.Uint64ToFloat64(fv.value)
}

func (fv *FieldValue) AsString() []byte {
	return fv.str
}

func (fv *FieldValue) Value() interface{} {
	switch fv.Type {
	case FieldValueTypeUnsigned:
		return fv.AsUint64()
	case FieldValueTypeSigned:
		return fv.AsInt64()
	case FieldValueTypeFloat:
		return fv.AsFloat64()
	case FieldValueTypeString:
		return fv.AsString()
	default: // FieldValueTypeNull
		return nil
	}
}
