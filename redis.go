package rds

import (
	"context"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
	"strconv"
)

const (
	auxIndexListSortedSuffix    = ":secondary_idx_zset_list"
	auxIndexListNonSortedSuffix = ":secondary_idx_set_list"
	// TODO - can we use something already existing?
	allIDsSuffix = "all_ids"

	// TODO - Do we need them?
	IDField      = "__id__"
	ETagField    = "__etag__"
	updatedField = "__updated__"
	payloadField = "__payload__"

	// TODO - from time const?
	dateTimeFormat = "2006-01-02 15:04:05.99999999 -0700 MST"
)

// Handler handles resource storage in Redis.
type Handler struct {
	client     *redis.Client
	entityName string
	// TODO - not needed with json
	fieldNames []string
	// needed to determine what secondary indices we are going to create to allow filtering (see predicate.go).
	filterable []string
	// needed for SORT type determination.
	numeric  []string
	sortable []string
}

// NewHandler creates a new redis handler
func NewHandler(c *redis.Client, entityName string, schema schema.Schema) *Handler {
	var names, filterable, sortable, numeric []string

	// add ETag explicitly - it's not in schema.Fields
	// TODO - do we need names?
	names = append(names, ETagField, payloadField)

	// TODO - better?
	for k, v := range schema.Fields {
		// ID is always filterable - needed for queries.
		if k == "id" {
			filterable = append(filterable, k)
		} else if v.Filterable {
			filterable = append(filterable, k)
		}

		// TODO - other specifics like ID?
		if v.Sortable {
			sortable = append(sortable, k)
		}

		// Detect possible numeric-value fields
		// TODO - don't use reflection? Use isNumeric?
		t := fmt.Sprintf("%T", v.Validator)
		if t == "Integer" || t == "Float" || t == "Time" {
			numeric = append(numeric, k)
		}
	}

	return &Handler{
		client:     c,
		entityName: entityName,
		fieldNames: []string{IDField, ETagField, payloadField, updatedField},
		filterable: filterable,
		sortable:   sortable,
		numeric:    numeric,
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
		// TODO - bulk inserts are not supported by REST-layer now
		// TODO: is atomic? Add WATCH?
		duplicates, err := h.client.Exists(ids...).Result()
		// TODO: is it real not found???
		if err != nil {
			return err
		}
		if duplicates > 0 {
			return resource.ErrConflict
		}

		pipe := h.client.TxPipeline()

		// Add hash-records
		for _, item := range items {
			key, value := h.newRedisItem(item)
			pipe.HMSet(key, value)
			// Add secondary indices for filterable fields
			h.addSecondaryIndices(pipe, item)
			h.addIDToAllIDsSet(pipe, item)
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

		// TODO: original?
		// TODO - is it atomic?
		if err := h.checkPresenceAndETag(key, original); err != nil {
			return err
		}

		pipe := h.client.TxPipeline()
		// TODO: HSet?
		pipe.HMSet(key, value)

		h.deleteSecondaryIndices(pipe, original)
		h.addSecondaryIndices(pipe, item)

		// TODO - we need it?
		h.deleteIDFromAllIDsSet(pipe, item)
		h.addIDToAllIDsSet(pipe, original)

		_, err := pipe.Exec()
		return err
	})

	return err
}

// Delete deletes an item from Redis
func (h Handler) Delete(ctx context.Context, item *resource.Item) error {
	err := handleWithContext(ctx, func() error {
		key, _ := h.newRedisItem(item)

		// TODO - is it atomic?
		if err := h.checkPresenceAndETag(key, item); err != nil {
			return err
		}

		pipe := h.client.TxPipeline()
		pipe.Del(h.redisItemKey(item))

		// todo - is it atomic?
		h.deleteSecondaryIndices(pipe, item)
		h.deleteIDFromAllIDsSet(pipe, item)

		_, err := pipe.Exec()
		return err
	})

	return err
}

// Clear purges all items from Redis matching the query
func (h Handler) Clear(ctx context.Context, q *query.Query) (int, error) {
	result := -1
	err := handleWithContext(ctx, func() error {
		luaQuery := new(LuaQuery)

		if err := luaQuery.addSelect(h.entityName, q); err != nil {
			return err
		}

		luaQuery.addDelete(h.entityName)

		var err error
		var res interface{}
		qs := redis.NewScript(luaQuery.Script)
		res, err = qs.Run(h.client, []string{}).Result()
		if err != nil {
			return err
		}

		// TODO - remove all IDs set

		// TODO - make better
		result, err = strconv.Atoi(fmt.Sprintf("%d", res))
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

		// TODO: implement properly
		var items = []*resource.Item{}
		d := data.([]interface{})

		// chunk data by items
		chunk := len(h.fieldNames)
		for i := 0; i < len(d); i += chunk {
			v := d[i : i+chunk]
			items = append(items, h.newItem(v))
		}

		// TODO - is len(items) correct?
		result = &resource.ItemList{
			Total: len(items),
			Limit: limit,
			Items: items,
		}

		return nil
	})
	return result, err
}

// checkPresenceAndETag checks if record is stored in DB (by its ID) and its ETag is the same as ETag in provided item.
// If no result found - no item is stored in the DB.
// If found - we should compare ETags.
func (h *Handler) checkPresenceAndETag(key string, item *resource.Item) error {
	current, err := h.client.HGet(key, ETagField).Result()
	// TODO: is it a real not found???
	if err != nil || current == "" {
		return resource.ErrNotFound
	}
	// TODO: make type-assertion?
	if string(current) != item.ETag {
		return resource.ErrConflict
	}
	return nil
}
