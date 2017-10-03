package redisl

import (
	"context"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
)

// Handler handles resource storage in Redis.
type Handler struct {
	client *redis.Client
	entityName string
	schema schema.Schema
}

// NewHandler creates a new redis handler
func NewHandler(c *redis.Client, entityName string, schema schema.Schema) *Handler {
	return &Handler{
		client: c,
		entityName: entityName,
		schema: schema,
	}
}

// Insert inserts new items in the Redis database
func (h *Handler) Insert(ctx context.Context, items []*resource.Item) error {
	err := handleWithContext(ctx, func() error {
		//pipe := h.client.Pipeline()
		// Check for duplicates with a bulk request
		var ids []string
		for _, item := range items {
			ids = append(ids, h.redisItemKey(item))
		}
		duplicates, err := h.client.Exists(ids...).Result()
		if err != nil {
			return err
		}
		if duplicates > 0 {
			return resource.ErrConflict
		}

		for _, item := range items {
			key, value := h.newRedisItem(item)
			err := h.client.HMSet(key, value).Err()
			if err != nil {
				return fmt.Errorf("Insert error on item %#v", item)
			}
		}
		return err
	})

	return err
}

// Update replace an item by a new one in Redis
func (h Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	return fmt.Errorf("j")
}

// Delete deletes an item from Redis
func (h Handler) Delete(ctx context.Context, item *resource.Item) error {
	return fmt.Errorf("j")
}

// Clear clears all items from Redis matching the query
func (h Handler) Clear(ctx context.Context, q *query.Query) (int, error) {
	return 0, fmt.Errorf("j")
}

// Find items from Redis matching the provided query
func (h Handler) Find(ctx context.Context, q *query.Query) (*resource.ItemList, error) {
	return &resource.ItemList{}, fmt.Errorf("j")
}

// newRedisItem converts a resource.Item into a suitable for go-redis HMSet [key, value] pair
func (h *Handler) newRedisItem(i *resource.Item) (string, map[string]interface{}) {
	// Filter out id from the payload so we don't store it twice
	value := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			value[k] = v
		}
	}
	value["__id__"] = i.ID
	value["__etag__"] = i.ETag
	value["__updated__"] = i.Updated

	return h.redisItemKey(i), value
}

// redisItemKey creates a redis-compatible string key from and for the resource item.
func (h *Handler) redisItemKey(i *resource.Item) string {
	return fmt.Sprintf("%s:%s", h.entityName, i.ID)
}

// handleWithContext makes requests to Redis aware of context.
// Additionally it checks if we already have context error before proceeding further.
// Rationale: redis-go actually doesn't support context abortion on its operations, though it has WithContext() client.
// See: https://github.com/go-redis/redis/issues/582
func handleWithContext(ctx context.Context, handler func() error) error {
	var err error

	if err = ctx.Err(); err != nil {
		return err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		err = handler()
	}()

	select {
	case <-ctx.Done():
		// Monitor context cancellation. cancellation may happen if the client closed the connection
		// or if the configured request timeout has been reached.
		return ctx.Err()
	case <-done:
		// Wait until Redis command finishes.
		return err
	}
}
