package mysql

import (
	"github.com/pingcap/check"
)

type mariaDBTestSuite struct {
}

var _ = check.Suite(&mariaDBTestSuite{})

func (t *mariaDBTestSuite) SetUpSuite(c *check.C) {

}

func (t *mariaDBTestSuite) TearDownSuite(c *check.C) {

}

func (t *mariaDBTestSuite) TestParseMariaDBGTID(c *check.C) {
	cases := []struct {
		gtidStr   string
		hashError bool
	}{
		{"0-1-1", false},
		{"", false},
		{"0-1-1-1", true},
		{"1", true},
		{"0-1-seq", true},
	}

	for _, cs := range cases {
		gtid, err := ParseMariadbGTID(cs.gtidStr)
		if cs.hashError {
			c.Assert(err, check.NotNil)
		} else {
			c.Assert(err, check.IsNil)
			c.Assert(gtid.String(), check.Equals, cs.gtidStr)
		}
	}
}

func (t *mariaDBTestSuite) TestMariaDBGTIDConatin(c *check.C) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		contain                     bool
	}{
		{"0-1-1", "0-1-2", false},
		{"0-1-1", "", true},
		{"2-1-1", "1-1-1", false},
		{"1-2-1", "1-1-1", true},
		{"1-2-2", "1-1-1", true},
	}

	for _, cs := range cases {
		originGTID, err := ParseMariadbGTID(cs.originGTIDStr)
		c.Assert(err, check.IsNil)
		otherGTID, err := ParseMariadbGTID(cs.otherGTIDStr)
		c.Assert(err, check.IsNil)

		c.Assert(originGTID.Contain(otherGTID), check.Equals, cs.contain)
	}
}

func (t *mariaDBTestSuite) TestMariaDBGTIDClone(c *check.C) {
	gtid, err := ParseMariadbGTID("1-1-1")
	c.Assert(err, check.IsNil)

	clone := gtid.Clone()
	c.Assert(gtid, check.DeepEquals, clone)
}

func (t *mariaDBTestSuite) TestMariaDBForward(c *check.C) {
	cases := []struct {
		currentGTIDStr, newerGTIDStr string
		hashError                    bool
	}{
		{"0-1-1", "0-1-2", false},
		{"0-1-1", "", false},
		{"2-1-1", "1-1-1", true},
		{"1-2-1", "1-1-1", false},
		{"1-2-2", "1-1-1", false},
	}

	for _, cs := range cases {
		currentGTID, err := ParseMariadbGTID(cs.currentGTIDStr)
		c.Assert(err, check.IsNil)
		newerGTID, err := ParseMariadbGTID(cs.newerGTIDStr)
		c.Assert(err, check.IsNil)

		err = currentGTID.forward(newerGTID)
		if cs.hashError {
			c.Assert(err, check.NotNil)
			c.Assert(currentGTID.String(), check.Equals, cs.currentGTIDStr)
		} else {
			c.Assert(err, check.IsNil)
			c.Assert(currentGTID.String(), check.Equals, cs.newerGTIDStr)
		}
	}
}

func (t *mariaDBTestSuite) TestParseMariaDBGTIDSet(c *check.C) {
	cases := []struct {
		gtidStr     string
		subGTIDs    map[uint32]string //domain ID => gtid string
		expectedStr []string          // test String()
		hasError    bool
	}{
		{"0-1-1", map[uint32]string{0: "0-1-1"}, []string{"0-1-1"}, false},
		{"", nil, []string{""}, false},
		{"0-1-1,1-2-3", map[uint32]string{0: "0-1-1", 1: "1-2-3"}, []string{"0-1-1,1-2-3", "1-2-3,0-1-1"}, false},
		{"0-1--1", nil, nil, true},
	}

	for _, cs := range cases {
		gtidSet, err := ParseMariadbGTIDSet(cs.gtidStr)
		if cs.hasError {
			c.Assert(err, check.NotNil)
		} else {
			c.Assert(err, check.IsNil)
			mariadbGTIDSet, ok := gtidSet.(*MariadbGTIDSet)
			c.Assert(ok, check.IsTrue)

			// check sub gtid
			c.Assert(mariadbGTIDSet.Sets, check.HasLen, len(cs.subGTIDs))
			for domainID, gtid := range mariadbGTIDSet.Sets {
				c.Assert(mariadbGTIDSet.Sets, check.HasKey, domainID)
				c.Assert(gtid.String(), check.Equals, cs.subGTIDs[domainID])
			}

			// check String() function
			inExpectedResult := false
			actualStr := mariadbGTIDSet.String()
			for _, str := range cs.expectedStr {
				if str == actualStr {
					inExpectedResult = true
					break
				}
			}
			c.Assert(inExpectedResult, check.IsTrue)
		}
	}
}

func (t *mariaDBTestSuite) TestMariaDBGTIDSetUpdate(c *check.C) {
	cases := []struct {
		isNilGTID bool
		gtidStr   string
		subGTIDs  map[uint32]string
	}{
		{true, "", map[uint32]string{1: "1-1-1", 2: "2-2-2"}},
		{false, "1-2-2", map[uint32]string{1: "1-2-2", 2: "2-2-2"}},
		{false, "1-2-1", map[uint32]string{1: "1-2-1", 2: "2-2-2"}},
		{false, "3-2-1", map[uint32]string{1: "1-1-1", 2: "2-2-2", 3: "3-2-1"}},
	}

	for _, cs := range cases {
		gtidSet, err := ParseMariadbGTIDSet("1-1-1,2-2-2")
		c.Assert(err, check.IsNil)
		mariadbGTIDSet, ok := gtidSet.(*MariadbGTIDSet)
		c.Assert(ok, check.IsTrue)

		if cs.isNilGTID {
			c.Assert(mariadbGTIDSet.AddSet(nil), check.IsNil)
		} else {
			err := gtidSet.Update(cs.gtidStr)
			c.Assert(err, check.IsNil)
		}
		// check sub gtid
		c.Assert(mariadbGTIDSet.Sets, check.HasLen, len(cs.subGTIDs))
		for domainID, gtid := range mariadbGTIDSet.Sets {
			c.Assert(mariadbGTIDSet.Sets, check.HasKey, domainID)
			c.Assert(gtid.String(), check.Equals, cs.subGTIDs[domainID])
		}
	}
}

func (t *mariaDBTestSuite) TestMariaDBGTIDSetEqual(c *check.C) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		equals                      bool
	}{
		{"", "", true},
		{"1-1-1", "1-1-1,2-2-2", false},
		{"1-1-1,2-2-2", "1-1-1", false},
		{"1-1-1,2-2-2", "1-1-1,2-2-2", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-3", false},
	}

	for _, cs := range cases {
		originGTID, err := ParseMariadbGTIDSet(cs.originGTIDStr)
		c.Assert(err, check.IsNil)

		otherGTID, err := ParseMariadbGTIDSet(cs.otherGTIDStr)
		c.Assert(err, check.IsNil)

		c.Assert(originGTID.Equal(otherGTID), check.Equals, cs.equals)
	}
}

func (t *mariaDBTestSuite) TestMariaDBGTIDSetContain(c *check.C) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		contain                     bool
	}{
		{"", "", true},
		{"1-1-1", "1-1-1,2-2-2", false},
		{"1-1-1,2-2-2", "1-1-1", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-2", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-1", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-3", false},
	}

	for _, cs := range cases {
		originGTIDSet, err := ParseMariadbGTIDSet(cs.originGTIDStr)
		c.Assert(err, check.IsNil)

		otherGTIDSet, err := ParseMariadbGTIDSet(cs.otherGTIDStr)
		c.Assert(err, check.IsNil)

		c.Assert(originGTIDSet.Contain(otherGTIDSet), check.Equals, cs.contain)
	}
}

func (t *mariaDBTestSuite) TestMariaDBGTIDSetClone(c *check.C) {
	cases := []string{"", "1-1-1", "1-1-1,2-2-2"}

	for _, str := range cases {
		gtidSet, err := ParseMariadbGTIDSet(str)
		c.Assert(err, check.IsNil)

		c.Assert(gtidSet.Clone(), check.DeepEquals, gtidSet)
	}
}
