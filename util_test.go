package rds

import (
	"fmt"
	"testing"

	"github.com/rs/rest-layer/schema/query"
	"github.com/stretchr/testify/assert"
)

func TestIsNumeric(t *testing.T) {
	cases := []struct {
		value query.Value
		want  bool
	}{
		{query.Value{6}, true},
		{query.Value{-6}, true},
		{query.Value{1.89}, true},
		{query.Value{-1.89}, true},
		{query.Value{6.0}, true},
		{query.Value{-6.0}, true},
		{query.Value{99999999999999999}, true},
		{query.Value{"foo"}, false},
		{query.Value{"1"}, false},
		{query.Value{"-3"}, false},
		{query.Value{"7.9"}, false},
		{query.Value{map[int]int{90: 900}}, false},
		{query.Value{[]int{90}}, false},
		{query.Value{query.Value{}}, false},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, isNumeric(tc.value), fmt.Sprintf("Test case #%d", i))
	}
}

func TestMakeLuaTableFromStrings(t *testing.T) {
	cases := []struct {
		value []string
		want  string
	}{
		{[]string{"aa"}, "{aa}"},
		{[]string{}, ""},
		{[]string{"a", "b"}, "{a,b}"},
		{[]string{"a.a", "b.b"}, "{a.a,b.b}"},
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
		value query.Value
		want  string
	}{
		{[]query.Value{"aa"}, "{aa}"},
		{[]query.Value{"aa", "b"}, "{aa,b}"},
		{[]query.Value{}, ""},
		{[]query.Value{2}, "{2}"},
		{[]query.Value{2, 4, 7}, "{2,4,7}"},
		{[]query.Value{2.0, 4.8, 7}, "{2.0,4.8,7}"},
		{[]query.Value{"a.a", "b.b"}, "{a.a,b.b}"},
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

func TestSKey(t *testing.T) {
	cases := []struct {
		entity string
		key    string
		value  interface{}
		want   string
	}{
		{"users", "1234", 78, "users:1234:78"},
		{"users", "bob", "78", "users:bob:78"},
		{"users", "bob", "78-90", "users:bob:78-90"},
		{"users:students", "bob", "78-90", "users:students:bob:78-90"},
		{"users_students", "bob", "78-90", "users_students:bob:78-90"},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, sKey(tc.entity, tc.key, tc.value), fmt.Sprintf("Test case #%d", i))
	}
}

func TestZKey(t *testing.T) {
	cases := []struct {
		entity string
		key    string
		want   string
	}{
		{"users", "bob", "users:bob"},
		{"users", "bob", "users:bob"},
		{"users:students", "bob", "users:students:bob"},
		{"users_students", "bob", "users_students:bob"},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, zKey(tc.entity, tc.key), fmt.Sprintf("Test case #%d", i))
	}
}

func TestSKeyLastAll(t *testing.T) {
	cases := []struct {
		entity string
		key    string
		want   string
	}{
		{"users", "bob", "users:bob:*"},
		{"users", "bob", "users:bob:*"},
		{"users:students", "bob", "users:students:bob:*"},
		{"users_students", "bob", "users_students:bob:*"},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, sKeyLastAll(tc.entity, tc.key), fmt.Sprintf("Test case #%d", i))
	}
}

func TestSIDsKey(t *testing.T) {
	cases := []struct {
		entity string
		want   string
	}{
		{"users", "users:ids"},
		{"users:students", "users:students:ids"},
		{"users_students", "users_students:ids"},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, sIDsKey(tc.entity), fmt.Sprintf("Test case #%d", i))
	}
}
