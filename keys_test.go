package rds

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestSKeyIDsAll(t *testing.T) {
	cases := []struct {
		entity string
		want   string
	}{
		{"users", "users:all_ids"},
		{"", ":all_ids"},
		{"users:students", "users:students:all_ids"},
		{"users_students", "users_students:all_ids"},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, sKeyIDsAll(tc.entity), fmt.Sprintf("Test case #%d", i))
	}
}

func TestAuxIndexListKey(t *testing.T) {
	cases := []struct {
		id     string
		sorted bool
		want   string
	}{
		{"users:123", true, "users:123:secondary_idx_zset_list"},
		{"users:123", false, "users:123:secondary_idx_set_list"},
		{"", true, ":secondary_idx_zset_list"},
		{"", false, ":secondary_idx_set_list"},
		{"users:", true, "users::secondary_idx_zset_list"},
		{"users_foo:abcdef123", false, "users_foo:abcdef123:secondary_idx_set_list"},
	}
	for i, tc := range cases {
		assert.Equal(t, tc.want, auxIndexListKey(tc.id, tc.sorted), fmt.Sprintf("Test case #%d", i))
	}
}
