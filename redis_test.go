package rds_test

import (
	"testing"

	rds "github.com/kolotaev/rest-layer-redis"

	"fmt"
	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
)

const CONFIG = &redis.Options{
	Addr: "127.0.0.1:6379",
}

type cleanupItem struct {
	values []*resource.Item
	schema schema.Schema
	entity string
}

// cleanup deletes all the specified items
func cleanup(items ...cleanupItem) {
	client := redis.NewClient(CONFIG)
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

func TestDelete(t *testing.T) {

}

func TestInsert(t *testing.T) {

}

func TestUpdate(t *testing.T) {

}

func TestClear(t *testing.T) {

}

func TestFind(t *testing.T) {

}
