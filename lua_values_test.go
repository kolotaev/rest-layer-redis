package rds

import (
	"fmt"
	"testing"
	"time"

	"github.com/rs/rest-layer/schema/query"
	"github.com/stretchr/testify/assert"
)

func TestGetRangeNumericPairs(t *testing.T) {
	cases := []struct {
		value []query.Value
		want  []string
		wantError bool
	}{
		{[]query.Value{}, []string{"{'-inf','+inf'}"}, false},
		{[]query.Value{50, -1, 23}, []string{"{'-inf',-1}", "{-1,23}", "{23,50}", "{50,'+inf'}"}, false},
		{[]query.Value{-1.5, 88.9007, 9999999.9}, []string{"{'-inf',-1.500000}", "{-1.500000,88.900700}", "{88.900700,9999999.900000}", "{9999999.900000,'+inf'}"}, false},
		{[]query.Value{555}, []string{"{'-inf',555}", "{555,'+inf'}"}, false},
		{[]query.Value{120.55}, []string{"{'-inf',120.550000}", "{120.550000,'+inf'}"}, false},

		{[]query.Value{120.55, 10}, []string{}, true},
		{[]query.Value{45, "10"}, []string{}, true},
		{[]query.Value{time.Now(), time.Now()}, []string{}, true},
	}
	for i, tc := range cases {
		res, err := getRangeNumericPairs(tc.value)
		tcm := fmt.Sprintf("Test case #%d", i)
		if tc.wantError {
			assert.Error(t, assert.AnError, tcm)
		} else {
			assert.NoError(t, err, tcm)
		}
		assert.Equal(t, tc.want, res, tcm)
	}
}

func TestMakeLuaTableFromStrings(t *testing.T) {
	cases := []struct {
		value []string
		want  string
	}{
		{[]string{"aa"}, "{'aa'}"},
		{[]string{}, "{}"},
		{[]string{""}, "{''}"},
		{[]string{"a", "b"}, "{'a','b'}"},
		{[]string{"a.a", "b.b"}, "{'a.a','b.b'}"},
		{[]string{"a.a", "b.b", "c.c"}, "{'a.a','b.b','c.c'}"},
	}
	for i, tc := range cases {
		assert.Equal(t, tc.want, makeLuaTableFromStrings(tc.value), fmt.Sprintf("Test case #%d", i))
	}
}

func TestMakeLuaTableFromValues(t *testing.T) {
	cases := []struct {
		value []query.Value
		want  string
	}{
		{[]query.Value{"aa"}, "{'aa'}"},
		{[]query.Value{"aa", "b"}, "{'aa','b'}"},
		{[]query.Value{}, "{}"},
		{[]query.Value{2}, "{2}"},
		{[]query.Value{2, 4, 7}, "{2,4,7}"},
		{[]query.Value{2.0, 4.8, 7}, "{2,4.8,7}"},
		{[]query.Value{"a.a", "b.b"}, "{'a.a','b.b'}"},
		{[]query.Value{true, false}, "{'true','false'}"},
	}
	for i, tc := range cases {
		assert.Equal(t, tc.want, makeLuaTableFromValues(tc.value), fmt.Sprintf("Test case #%d", i))
	}
}

func TestTmpVar(t *testing.T) {
	v1 := tmpVar()
	v2 := tmpVar()
	v3 := tmpVar()
	assert.NotEqual(t, v1, v2)
	assert.NotEqual(t, v1, v3)
	assert.NotEqual(t, v2, v3)
}

func TestQuoteValue(t *testing.T) {
	cases := []struct {
		value interface{}
		want  string
	}{
		{true, "'true'"},
		{"foo", "'foo'"},
		{45, "45"},
		{45.58, "45.58"},
	}
	for i, tc := range cases {
		assert.Equal(t, tc.want, quoteValue(tc.value), fmt.Sprintf("Test case #%d", i))
	}
}
