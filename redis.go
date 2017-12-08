package rds

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
	client     *redis.Client
	entityName string
	filterable []string
	numeric []string
	fieldNames 	[]string
}

// NewHandler creates a new redis handler
func NewHandler(c *redis.Client, entityName string, schema schema.Schema) *Handler {
	var names, filterable, numeric []string

	for k, v := range schema.Fields {
		names = append(names, k)

		// ID is always filterable - needed for queries.
		if k == "id" {
			filterable = append(filterable, k)
		}
		if v.Filterable {
			filterable = append(filterable, k)
		}

		// Detect possible numeric-value fields
		// We'll need the for SORT type determination.
		switch v.Validator.(type) {
		case schema.Integer, schema.Float:
			numeric = append(numeric, k)
		}
	}
	return &Handler{
		client:     c,
		entityName: entityName,
		filterable: filterable,
		fieldNames: names,
	}
}

// Insert inserts new items in the Redis database
func (h *Handler) Insert(ctx context.Context, items []*resource.Item) error {
	err := handleWithContext(ctx, func() error {
		// Check for duplicates with a bulk request
		var ids []string
		for _, item := range items {
			ids = append(ids, h.redisItemKey(item))
		}
		duplicates, err := h.client.Exists(ids...).Result()
		// TODO: is it real not found???
		if err != nil {
			return err
		}
		if duplicates > 0 {
			return resource.ErrConflict
		}

		pipe := h.client.Pipeline()
		// Add hash-records
		for _, item := range items {
			key, value := h.newRedisItem(item)
			pipe.HMSet(key, value)
		}
		// Add secondary indices for filterable fields
		for _, item := range items {
			for _, k := range h.getIndexSetKeys(item) {
				pipe.SAdd(k, h.redisItemKey(item))
			}
			for k, v := range h.getIndexZSetKeys(item) {
				pipe.ZAdd(k, redis.Z{Score: v, Member: h.redisItemKey(item)})
			}
		}
		_, err = pipe.Exec()
		return err
	})

	return err
}

// Update updates item properties in Redis
func (h Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	err := handleWithContext(ctx, func() error {
		key, value := h.newRedisItem(item)

		current, err := h.client.HMGet(key, "__etag__").Result()
		// TODO: is it real not found???
		if err != nil {
			return resource.ErrNotFound
		}
		// TODO: original?
		if current[0] != original.ETag {
			return resource.ErrConflict
		}

		pipe := h.client.Pipeline()
		pipe.HMSet(key, value)

		// Delete old values from a secondary index.
		for _, k := range h.getIndexSetKeys(original) {
			pipe.SRem(k, h.redisItemKey(original))
		}
		for k := range h.getIndexZSetKeys(original) {
			pipe.ZRem(k, h.redisItemKey(original))
		}

		// Add new values to a secondary index.
		for _, k := range h.getIndexSetKeys(item) {
			pipe.SAdd(k, h.redisItemKey(item))
		}
		for k, v := range h.getIndexZSetKeys(item) {
			pipe.ZAdd(k, redis.Z{Score: v, Member: h.redisItemKey(item)})
		}
		_, err = pipe.Exec()
		return err
	})

	return err
}

// Delete deletes an item from Redis
func (h Handler) Delete(ctx context.Context, item *resource.Item) error {
	err := handleWithContext(ctx, func() error {
		key, _ := h.newRedisItem(item)

		current, err := h.client.HMGet(key, "__etag__").Result()
		// TODO: is it real not found???
		if err != nil {
			return resource.ErrNotFound
		}
		if current[0] != item.ETag {
			return resource.ErrConflict
		}

		pipe := h.client.Pipeline()
		pipe.HDel(h.redisItemKey(item))

		// todo - is it atomic?
		// Delete secondary indices.
		for _, k := range h.getIndexSetKeys(item) {
			pipe.SRem(k, h.redisItemKey(item))
		}
		for k := range h.getIndexZSetKeys(item) {
			pipe.ZRem(k, h.redisItemKey(item))
		}
		_, err = pipe.Exec()
		return err
	})

	return err
}

// Clear clears all items from Redis matching the query
func (h Handler) Clear(ctx context.Context, q *query.Query) (int, error) {
	result := -1
	err := handleWithContext(ctx, func() error {
		luaQuery := &LuaQuery{}

		if err := luaQuery.addSelect(h.entityName, q); err != nil {
			return err
		}

	  	luaQuery.addDelete()

		var err error
		qs := redis.NewScript(luaQuery.Script)
		result, err = qs.Run(h.client, []string{}).Result()
		if err != nil {
			return err
		}
		return nil
	})
	return result, err
}

// Find items from Redis matching the provided query
func (h Handler) Find(ctx context.Context, q *query.Query) (*resource.ItemList, error) {
	var result *resource.ItemList

	err := handleWithContext(ctx, func() error {
		luaQuery := &LuaQuery{}
		if err := luaQuery.addSelect(h.entityName, q); err != nil {
			return err
		}

		limit, offset := -1, 0
		if q.Window != nil {
			if q.Window.Limit >= 0 {
				limit = q.Window.Limit
			}
			if q.Window.Offset > 0 {
				offset = q.Window.Offset
			}
		}

		if err := luaQuery.addSortWithLimit(q, limit, offset, h.fieldNames, h.numeric); err != nil {
			return err
		}

		qs := redis.NewScript(luaQuery.Script)
		data, err := qs.Run(h.client, []string{}, "value").Result()
		if err != nil {
			return err
		}

		result = &resource.ItemList{
			Total: -1,
			Limit: limit,
			Items: []*resource.Item{},
		}

		for _, v := range data {
			result.Items = append(result.Items, h.newItem(v))
		}

		return nil
	})
	return result, err
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

// newItem converts a Redis item from DB into resource.Item
func (h *Handler) newItem(i interface{}) *resource.Item {
	return &resource.Item{}
}

// getIndexZSetKeys creates a secondary index keys for a resource's filterable fields suited for ZSET.
// Is used so that we can find them when needed.
// Ex: for users item returns {"users:age": 56, "users:salary": 8000}
func (h *Handler) getIndexZSetKeys(i *resource.Item) map[string]float64 {
	result := make(map[string]float64)
	for _, field := range h.filterable {
		if value, ok := i.Payload[field]; ok && isNumeric(value) {
			result[zKey(h.entityName, field)] = value.(float64)
		}
	}
	return result
}

// getIndexSetKeys creates a secondary index keys for a resource's filterable fields suited for SET.
// Is used so that we can find them when needed.
// Ex: for users item returns ["users:hair:brown", "users:city:NYC"]
func (h *Handler) getIndexSetKeys(i *resource.Item) []string {
	var result []string
	for _, field := range h.filterable {
		if value, ok := i.Payload[field]; ok && !isNumeric(value) {
			result = append(result, sKey(h.entityName, field, value))
		}
	}
	return result
}

// redisItemKey creates a redis-compatible string key to denote a Hash key of an item.
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
