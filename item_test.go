package rds

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuxIndexListKey(t *testing.T) {
	cases := []struct {
		id    string
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
