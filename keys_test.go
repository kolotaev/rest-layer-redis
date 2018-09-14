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
