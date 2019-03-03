package toml

import (
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"
)

func init() {
	log.SetFlags(0)
}

func TestDecodeSimple(t *testing.T) {
	var testSimple = `
age = 250
andrew = "gallant"
kait = "brady"
now = 1987-07-05T05:45:00Z 
yesOrNo = true
pi = 3.14
colors = [
	["red", "green", "blue"],
	["cyan", "magenta", "yellow", "black"],
]

[My.Cats]
plato = "cat 1"
cauchy = "cat 2"
`

	type cats struct {
		Plato  string
		Cauchy string
	}
	type simple struct {
		Age     int
		Colors  [][]string
		Pi      float64
		YesOrNo bool
		Now     time.Time
		Andrew  string
		Kait    string
		My      map[string]cats
	}

	var val simple
	_, err := Decode(testSimple, &val)
	if err != nil {
		t.Fatal(err)
	}

	now, err := time.Parse("2006-01-02T15:04:05", "1987-07-05T05:45:00")
	if err != nil {
		panic(err)
	}
	var answer = simple{
		Age:     250,
		Andrew:  "gallant",
		Kait:    "brady",
		Now:     now,
		YesOrNo: true,
		Pi:      3.14,
		Colors: [][]string{
			{"red", "green", "blue"},
			{"cyan", "magenta", "yellow", "black"},
		},
		My: map[string]cats{
			"Cats": cats{Plato: "cat 1", Cauchy: "cat 2"},
		},
	}
	if !reflect.DeepEqual(val, answer) {
		t.Fatalf("Expected\n-----\n%#v\n-----\nbut got\n-----\n%#v\n",
			answer, val)
	}
}

func TestDecodeEmbedded(t *testing.T) {
	type Dog struct{ Name string }
	type Age int

	tests := map[string]struct {
		input       string
		decodeInto  interface{}
		wantDecoded interface{}
	}{
		"embedded struct": {
			input:       `Name = "milton"`,
			decodeInto:  &struct{ Dog }{},
			wantDecoded: &struct{ Dog }{Dog{"milton"}},
		},
		"embedded non-nil pointer to struct": {
			input:       `Name = "milton"`,
			decodeInto:  &struct{ *Dog }{},
			wantDecoded: &struct{ *Dog }{&Dog{"milton"}},
		},
		"embedded nil pointer to struct": {
			input:       ``,
			decodeInto:  &struct{ *Dog }{},
			wantDecoded: &struct{ *Dog }{nil},
		},
		"embedded int": {
			input:       `Age = -5`,
			decodeInto:  &struct{ Age }{},
			wantDecoded: &struct{ Age }{-5},
		},
	}

	for label, test := range tests {
		_, err := Decode(test.input, test.decodeInto)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(test.wantDecoded, test.decodeInto) {
			t.Errorf("%s: want decoded == %+v, got %+v",
				label, test.wantDecoded, test.decodeInto)
		}
	}
}

func TestTableArrays(t *testing.T) {
	var tomlTableArrays = `
[[albums]]
name = "Born to Run"

  [[albums.songs]]
  name = "Jungleland"

  [[albums.songs]]
  name = "Meeting Across the River"

[[albums]]
name = "Born in the USA"
  
  [[albums.songs]]
  name = "Glory Days"

  [[albums.songs]]
  name = "Dancing in the Dark"
`

	type Song struct {
		Name string
	}

	type Album struct {
		Name  string
		Songs []Song
	}

	type Music struct {
		Albums []Album
	}

	expected := Music{[]Album{
		{"Born to Run", []Song{{"Jungleland"}, {"Meeting Across the River"}}},
		{"Born in the USA", []Song{{"Glory Days"}, {"Dancing in the Dark"}}},
	}}
	var got Music
	if _, err := Decode(tomlTableArrays, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("\n%#v\n!=\n%#v\n", expected, got)
	}
}

// Case insensitive matching tests.
// A bit more comprehensive than needed given the current implementation,
// but implementations change.
// Probably still missing demonstrations of some ugly corner cases regarding
// case insensitive matching and multiple fields.
func TestCase(t *testing.T) {
	var caseToml = `
tOpString = "string"
tOpInt = 1
tOpFloat = 1.1
tOpBool = true
tOpdate = 2006-01-02T15:04:05Z
tOparray = [ "array" ]
Match = "i should be in Match only"
MatcH = "i should be in MatcH only"
once = "just once"
[nEst.eD]
nEstedString = "another string"
`

	type InsensitiveEd struct {
		NestedString string
	}

	type InsensitiveNest struct {
		Ed InsensitiveEd
	}

	type Insensitive struct {
		TopString string
		TopInt    int
		TopFloat  float64
		TopBool   bool
		TopDate   time.Time
		TopArray  []string
		Match     string
		MatcH     string
		Once      string
		OncE      string
		Nest      InsensitiveNest
	}

	tme, err := time.Parse(time.RFC3339, time.RFC3339[:len(time.RFC3339)-5])
	if err != nil {
		panic(err)
	}
	expected := Insensitive{
		TopString: "string",
		TopInt:    1,
		TopFloat:  1.1,
		TopBool:   true,
		TopDate:   tme,
		TopArray:  []string{"array"},
		MatcH:     "i should be in MatcH only",
		Match:     "i should be in Match only",
		Once:      "just once",
		OncE:      "",
		Nest: InsensitiveNest{
			Ed: InsensitiveEd{NestedString: "another string"},
		},
	}
	var got Insensitive
	if _, err := Decode(caseToml, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("\n%#v\n!=\n%#v\n", expected, got)
	}
}

func TestPointers(t *testing.T) {
	type Object struct {
		Type        string
		Description string
	}

	type Dict struct {
		NamedObject map[string]*Object
		BaseObject  *Object
		Strptr      *string
		Strptrs     []*string
	}
	s1, s2, s3 := "blah", "abc", "def"
	expected := &Dict{
		Strptr:  &s1,
		Strptrs: []*string{&s2, &s3},
		NamedObject: map[string]*Object{
			"foo": {"FOO", "fooooo!!!"},
			"bar": {"BAR", "ba-ba-ba-ba-barrrr!!!"},
		},
		BaseObject: &Object{"BASE", "da base"},
	}

	ex1 := `
Strptr = "blah"
Strptrs = ["abc", "def"]

[NamedObject.foo]
Type = "FOO"
Description = "fooooo!!!"

[NamedObject.bar]
Type = "BAR"
Description = "ba-ba-ba-ba-barrrr!!!"

[BaseObject]
Type = "BASE"
Description = "da base"
`
	dict := new(Dict)
	_, err := Decode(ex1, dict)
	if err != nil {
		t.Errorf("Decode error: %v", err)
	}
	if !reflect.DeepEqual(expected, dict) {
		t.Fatalf("\n%#v\n!=\n%#v\n", expected, dict)
	}
}

type sphere struct {
	Center [3]float64
	Radius float64
}

func TestDecodeSimpleArray(t *testing.T) {
	var s1 sphere
	if _, err := Decode(`center = [0.0, 1.5, 0.0]`, &s1); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeArrayWrongSize(t *testing.T) {
	var s1 sphere
	if _, err := Decode(`center = [0.1, 2.3]`, &s1); err == nil {
		t.Fatal("Expected array type mismatch error")
	}
}

func TestDecodeLargeIntoSmallInt(t *testing.T) {
	type table struct {
		Value int8
	}
	var tab table
	if _, err := Decode(`value = 500`, &tab); err == nil {
		t.Fatal("Expected integer out-of-bounds error.")
	}
}

func TestDecodeSizedInts(t *testing.T) {
	type table struct {
		U8  uint8
		U16 uint16
		U32 uint32
		U64 uint64
		U   uint
		I8  int8
		I16 int16
		I32 int32
		I64 int64
		I   int
	}
	answer := table{1, 1, 1, 1, 1, -1, -1, -1, -1, -1}
	toml := `
	u8 = 1
	u16 = 1
	u32 = 1
	u64 = 1
	u = 1
	i8 = -1
	i16 = -1
	i32 = -1
	i64 = -1
	i = -1
	`
	var tab table
	if _, err := Decode(toml, &tab); err != nil {
		t.Fatal(err.Error())
	}
	if answer != tab {
		t.Fatalf("Expected %#v but got %#v", answer, tab)
	}
}

func ExampleMetaData_PrimitiveDecode() {
	var md MetaData
	var err error

	var tomlBlob = `
ranking = ["Springsteen", "J Geils"]

[bands.Springsteen]
started = 1973
albums = ["Greetings", "WIESS", "Born to Run", "Darkness"]

[bands.J Geils]
started = 1970
albums = ["The J. Geils Band", "Full House", "Blow Your Face Out"]
`

	type band struct {
		Started int
		Albums  []string
	}
	type classics struct {
		Ranking []string
		Bands   map[string]Primitive
	}

	// Do the initial decode. Reflection is delayed on Primitive values.
	var music classics
	if md, err = Decode(tomlBlob, &music); err != nil {
		log.Fatal(err)
	}

	// MetaData still includes information on Primitive values.
	fmt.Printf("Is `bands.Springsteen` defined? %v\n",
		md.IsDefined("bands", "Springsteen"))

	// Decode primitive data into Go values.
	for _, artist := range music.Ranking {
		// A band is a primitive value, so we need to decode it to get a
		// real `band` value.
		primValue := music.Bands[artist]

		var aBand band
		if err = md.PrimitiveDecode(primValue, &aBand); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s started in %d.\n", artist, aBand.Started)
	}
	// Check to see if there were any fields left undecoded.
	// Note that this won't be empty before decoding the Primitive value!
	fmt.Printf("Undecoded: %q\n", md.Undecoded())

	// Output:
	// Is `bands.Springsteen` defined? true
	// Springsteen started in 1973.
	// J Geils started in 1970.
	// Undecoded: []
}

func ExampleDecode() {
	var tomlBlob = `
# Some comments.
[alpha]
ip = "10.0.0.1"

	[alpha.config]
	Ports = [ 8001, 8002 ]
	Location = "Toronto"
	Created = 1987-07-05T05:45:00Z

[beta]
ip = "10.0.0.2"

	[beta.config]
	Ports = [ 9001, 9002 ]
	Location = "New Jersey"
	Created = 1887-01-05T05:55:00Z
`

	type serverConfig struct {
		Ports    []int
		Location string
		Created  time.Time
	}

	type server struct {
		IP     string       `toml:"ip"`
		Config serverConfig `toml:"config"`
	}

	type servers map[string]server

	var config servers
	if _, err := Decode(tomlBlob, &config); err != nil {
		log.Fatal(err)
	}

	for _, name := range []string{"alpha", "beta"} {
		s := config[name]
		fmt.Printf("Server: %s (ip: %s) in %s created on %s\n",
			name, s.IP, s.Config.Location,
			s.Config.Created.Format("2006-01-02"))
		fmt.Printf("Ports: %v\n", s.Config.Ports)
	}

	// Output:
	// Server: alpha (ip: 10.0.0.1) in Toronto created on 1987-07-05
	// Ports: [8001 8002]
	// Server: beta (ip: 10.0.0.2) in New Jersey created on 1887-01-05
	// Ports: [9001 9002]
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// Example Unmarshaler shows how to decode TOML strings into your own
// custom data type.
func Example_unmarshaler() {
	blob := `
[[song]]
name = "Thunder Road"
duration = "4m49s"

[[song]]
name = "Stairway to Heaven"
duration = "8m03s"
`
	type song struct {
		Name     string
		Duration duration
	}
	type songs struct {
		Song []song
	}
	var favorites songs
	if _, err := Decode(blob, &favorites); err != nil {
		log.Fatal(err)
	}

	// Code to implement the TextUnmarshaler interface for `duration`:
	//
	// type duration struct {
	// 	time.Duration
	// }
	//
	// func (d *duration) UnmarshalText(text []byte) error {
	// 	var err error
	// 	d.Duration, err = time.ParseDuration(string(text))
	// 	return err
	// }

	for _, s := range favorites.Song {
		fmt.Printf("%s (%s)\n", s.Name, s.Duration)
	}
	// Output:
	// Thunder Road (4m49s)
	// Stairway to Heaven (8m3s)
}

// Example StrictDecoding shows how to detect whether there are keys in the
// TOML document that weren't decoded into the value given. This is useful
// for returning an error to the user if they've included extraneous fields
// in their configuration.
func Example_strictDecoding() {
	var blob = `
key1 = "value1"
key2 = "value2"
key3 = "value3"
`
	type config struct {
		Key1 string
		Key3 string
	}

	var conf config
	md, err := Decode(blob, &conf)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Undecoded keys: %q\n", md.Undecoded())
	// Output:
	// Undecoded keys: ["key2"]
}
