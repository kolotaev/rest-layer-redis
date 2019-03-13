package rds_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/rs/rest-layer/resource"

	rds "github.com/kolotaev/rest-layer-redis"
)

func TestRedisItemKey(t *testing.T) {
	cases := []struct {
		entity string
		item *resource.Item
		want string
	}{
		{
			"users",
			&resource.Item{ID: "123"},
			"users:123",
		},
		{
			"users:foo:bar",
			&resource.Item{ID: "123"},
			"users:foo:bar:123",
		},
		{
			"users:",
			&resource.Item{ID: ""},
			"users::",
		},
		{
			"",
			&resource.Item{ID: ""},
			":",
		},
	}
	for i, tc := range cases {
		manager := &rds.ItemManager{
			EntityName: tc.entity,
		}
		assert.Equal(t, tc.want, manager.RedisItemKey(tc.item), fmt.Sprintf("Test case #%d", i))
	}
}
