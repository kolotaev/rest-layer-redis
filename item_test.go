package rds

import (
	//"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuxIndexListKey(t *testing.T) {
	//cases := []struct {
	//	key    string
	//	sorted bool
	//	want   string
	//}{
	//	{"users", true, "users:secondary_idx_zset_list"},
	//	{"users", false, "users:secondary_idx_set_list"},
	//	{"", true, ":secondary_idx_zset_list"},
	//	{"", false, ":secondary_idx_set_list"},
	//	{"users:students", true, "users:students:secondary_idx_zset_list"},
	//	{"users_students", false, "users_students:secondary_idx_set_list"},
	//}
	//for i := range cases {
	//	tc := cases[i]
	//	assert.Equal(t, tc.want, auxIndexListKey(tc.entity), fmt.Sprintf("Test case #%d", i))
	//}
	assert.True(t, false, "pending")
}
