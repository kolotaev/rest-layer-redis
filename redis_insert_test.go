package rds_test

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"

	rds "github.com/kolotaev/rest-layer-redis"
)

const REDIS_ADDRESS = "127.0.0.1:6379"

type cleanupItem struct {
	values []*resource.Item
	schema schema.Schema
	entity string
}

// cleanup deletes all the specified items
func cleanup(items ...cleanupItem) {
	client := redis.NewClient(&redis.Options{
		Addr: REDIS_ADDRESS,
	})
	_, err := client.Ping().Result()
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range items {
		h := rds.NewHandler(client, v.entity, v.schema)
		for _, val := range v.values {
			h.Delete(nil, val)
		}
	}
}


func TestInsert(t *testing.T) {

}
