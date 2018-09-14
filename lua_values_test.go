package rds

import (
	"fmt"
	"testing"

	"github.com/rs/rest-layer/schema/query"
	"github.com/stretchr/testify/assert"
)

func TestMakeLuaTableFromStrings(t *testing.T) {
	cases := []struct {
		value []string
		want  string
	}{
		{[]string{"aa"}, "{aa}"},
		{[]string{}, ""},
		{[]string{"a", "b"}, "{a,b}"},
		{[]string{"a.a", "b.b"}, "{a.a,b.b}"},
		{[]string{"a.a", "b.b", "c.c"}, "{a.a,b.b,c.c}"},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, makeLuaTableFromStrings(tc.value), fmt.Sprintf("Test case #%d", i))
	}
}

func TestGetRangePairs(t *testing.T) {
	cases := []struct {
		value []query.Value
		want  []string
	}{
		{[]query.Value{}, []string{"{-inf,+inf}"}},
		{[]query.Value{"s", "a", "r"}, []string{"{-inf,a}", "{a,s}", "{s,r}", "{r, +inf}"}},
		{[]query.Value{"a", "s", "ё"}, []string{"{-inf,a}", "{a,s}", "{s,ё}", "{ё, +inf}"}},
		{[]query.Value{8, 7, -9}, []string{"{-inf,-9}", "{-9,7}", "{7,8}", "{8, +inf}"}},
		{[]query.Value{555}, []string{"{-inf,555}", "{555, +inf}"}},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, getRangePairs(tc.value), fmt.Sprintf("Test case #%d", i))
	}
}

func TestMakeLuaTableFromValues(t *testing.T) {
	cases := []struct {
		value []query.Value
		want  string
	}{
		{[]query.Value{"aa"}, "{'aa'}"},
		{[]query.Value{"aa", "b"}, "{'aa','b'}"},
		{[]query.Value{}, ""},
		{[]query.Value{2}, "{2}"},
		{[]query.Value{2, 4, 7}, "{2,4,7}"},
		{[]query.Value{2.0, 4.8, 7}, "{2.0,4.8,7}"},
		{[]query.Value{"a.a", "b.b"}, "{'a.a','b.b'}"},
	}
	for i := range cases {
		tc := cases[i]
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
