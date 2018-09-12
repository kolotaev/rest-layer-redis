package rds

import (
	"context"
	"fmt"
	"errors"
	"time"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
)

const (
	auxIndexListSortedSuffix = ":secondary_idx_zset_list"
	auxIndexListNonSortedSuffix = ":secondary_idx_set_list"
	
	IDField = "__id__"
	ETagField = "__etag__"
	updatedField = "__updated__"

	// TODO - from time const?
	dateTimeFormat = "2006-01-02 15:04:05.99999999 -0700 MST"
)

// Handler handles resource storage in Redis.
type Handler struct {
	client     *redis.Client
	entityName string
	// needed to determine what secondary indices we are going to create to allow filtering (see predicate.go).
	filterable []string
	// needed for SORT type determination.
	numeric    []string
	fieldNames []string
}

// NewHandler creates a new redis handler
func NewHandler(c *redis.Client, entityName string, schema schema.Schema) *Handler {
	var names, filterable, numeric []string

	// add ETag explicitly - it's not in schema.Fields
	names = append(names, ETagField)

	// TODO - better?
	for k, v := range schema.Fields {
		if k == "id" {
			names = append(names, IDField)
		} else if k == "updated" {
			names = append(names, updatedField)
		} else {
			names = append(names, k)
		}

		// ID is always filterable - needed for queries.
		if k == "id" {
			filterable = append(filterable, k)
		} else if v.Filterable {
			filterable = append(filterable, k)
		}

		// Detect possible numeric-value fields
		// TODO - don't use reflection? Use isNumeric?
		t := fmt.Sprintf("%T", v.Validator)
		if t == "Integer" || t == "Float"  || t == "Time" {
			numeric = append(numeric, k)
		}
		//switch v.Validator.(type) {
		//case schema.Integer, schema.Float:
		//	numeric = append(numeric, k)
		//}
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
		}

		// Add secondary indices for filterable fields
		for _, item := range items {
			h.addSecondaryIndices(pipe, item)
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

		luaQuery.addDelete()

		var err error
		var res interface{}
		qs := redis.NewScript(luaQuery.Script)
		res, err = qs.Run(h.client, []string{}).Result()
		if err != nil {
			return err
		}

		// TODO - make better
		if resVal, ok := res.(int); !ok {
			return errors.New("Unknown result")
		} else {
			result = resVal
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

		for i := 0; i < len(d); i += len(h.fieldNames) {
			v := d[i:len(h.fieldNames)]
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

// newRedisItem converts a resource.Item into a suitable for go-redis HMSet [key, value] pair
func (h *Handler) newRedisItem(i *resource.Item) (string, map[string]interface{}) {
	value := map[string]interface{}{}

	for k, v := range i.Payload {
		// todo - maybe better time handling?
		if t, ok := v.(time.Time); ok {
			//t.Nanosecond()
			value[k] = t.Format(dateTimeFormat)
		} else if b, ok := v.(bool); ok {
			value[k] = fmt.Sprintf("%t", b)
		} else if k != "id" {
			// Filter out id from the payload so we don't store it twice
			value[k] = v
		}
	}

	value[IDField] = i.ID
	value[ETagField] = i.ETag
	// TODO we need em?
	value[updatedField] = i.Updated.String() // TODO -  time.Parse(dateTimeFormat, value). Move to parser

	return h.redisItemKey(i), value
}

// newItem converts a Redis item from DB into resource.Item
func (h *Handler) newItem(data interface{}) *resource.Item {
	pr("//////////", data)

	item := &resource.Item{
		Payload: make(map[string]interface{}),
	}

	aInterface, ok := data.([]interface{})
	if !ok {
		pr("not []interface{}")
		return nil
	}
	aString := make([]string, len(aInterface))
	for i, v := range aInterface {
		a, ok := v.(string)
		if !ok {
			pr("not string")
			return nil
		}
		aString[i] = a
	}

	for i, v := range h.fieldNames {
		// TODO - need this separation???
		value := aString[i]
		if v == IDField {
			item.ID = value
			continue
		}
		if v == ETagField {
			item.ETag = value
			continue
		}
		if v == updatedField {
			//i, err := strconv.ParseInt(value, 10, 64)
			//if err != nil {
			//	pr("failed to parse date")
			//} else {
			//	tm := time.Unix(i, 0)
			//	item.Updated = tm
			//}

			ut, err := time.Parse(dateTimeFormat, value)
			if err != nil {
				pr("failed to parse date")
			} else {
				item.Updated = ut
			}
			continue
		}

		// TODO - try to do parsing?
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			item.Payload[v] = i
			continue
		}
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			item.Payload[v] = f
			continue
		}
		// TODO - bools "1" "0"
		if b, err := strconv.ParseBool(value); err == nil {
			item.Payload[v] = b
			continue
		}
		if t, err := time.Parse(dateTimeFormat, value); err == nil {
			item.Payload[v] = t
			continue
		}
		item.Payload[v] = value
	}

	return item
}

// indexSetKeys returns a secondary index keys for a resource's filterable fields suited for SET.
// Is used so that we can find them when needed.
// Ex: for user A returns ["users:hair:brown", "users:city:NYC"]
//     for user B returns ["users:hair:red", "users:city:Boston"]
func (h *Handler) indexSetKeys(i *resource.Item) []string {
	var result []string
	for _, field := range h.filterable {
		if value, ok := i.Payload[field]; ok && !isNumeric(value) {
			pr("<<< SET", value)
			result = append(result, sKey(h.entityName, field, value))
		}
	}
	// TODO - do we need etag? Isn't ID already in filterable?
	result = append(result, sKey(h.entityName, "id", i.ID))
	return result
}

// indexZSetKeys returns a secondary index keys for a resource's filterable fields suited for ZSET.
// Is used so that we can find them when needed.
// Ex: for user A returns {"users:age": 24, "users:salary": 75000}
//     for user B returns {"users:age": 56, "users:salary": 125000}
func (h *Handler) indexZSetKeys(i *resource.Item) map[string]float64 {
	// TODO: float for all?
	result := make(map[string]float64)
	for _, field := range h.filterable {
		if value, ok := i.Payload[field]; ok && isNumeric(value) {
			pr("<<< ZSET", value)
			result[zKey(h.entityName, field)] = valueToFloat(value)
		}
	}
	// TODO - do we need etag? Isn't updated already in filterable?
	result[zKey(h.entityName, "updated")] = valueToFloat(i.Updated)

	return result
}

// addSecondaryIndices adds:
// - new values to a secondary index for a given item.
// - index names to a maintained auxiliary list of item's indices.
// Action is appended to a Redis pipeline.
func (h *Handler) addSecondaryIndices(pipe redis.Pipeliner, item *resource.Item) {
	var setIndexes, zSetIndexes []interface{}
	itemID := h.redisItemKey(item)
	for _, v := range h.indexSetKeys(item) {
		pipe.SAdd(v, itemID)
		setIndexes = append(setIndexes, v)
	}
	for k, v := range h.indexZSetKeys(item) {
		pipe.ZAdd(k, redis.Z{Member: itemID, Score: v})
		zSetIndexes = append(zSetIndexes, k)
	}
	if len(setIndexes) > 0 {
		pipe.SAdd(h.auxIndexListKey(itemID, false), setIndexes...)
	}
	if len(zSetIndexes) > 0 {
		pipe.SAdd(h.auxIndexListKey(itemID, true), zSetIndexes...)
	}
}

// deleteSecondaryIndices removes:
// - a secondary index for a given item.
// - index names to a maintained auxiliary list of item's indices.
// Action is appended to a Redis pipeline.
func (h *Handler) deleteSecondaryIndices(pipe redis.Pipeliner, item *resource.Item) {
	var setIndexes, zSetIndexes []interface{}
	itemID := h.redisItemKey(item)
	for _, v := range h.indexSetKeys(item) {
		pipe.SRem(v, itemID)
		setIndexes = append(setIndexes, v)
	}
	for k := range h.indexZSetKeys(item) {
		pipe.ZRem(k, itemID)
		zSetIndexes = append(zSetIndexes, k)
	}
	// TODO - shouldn't we delete the entire list?
	if len(setIndexes) > 0 {
		pipe.SRem(h.auxIndexListKey(itemID, false), setIndexes...)
	}
	if len(zSetIndexes) > 0 {
		pipe.SRem(h.auxIndexListKey(itemID, true), zSetIndexes...)
	}
}

// redisItemKey returns a redis-compatible string key to denote a Hash key of an item. E.g. 'users:1234'.
func (h *Handler) redisItemKey(i *resource.Item) string {
	return fmt.Sprintf("%s:%s", h.entityName, i.ID)
}

// auxIndexListKey returns a redis-compatible string key to denote a name of an auxiliary indices list of an Item.
func (h *Handler) auxIndexListKey(key string, sorted bool) string {
	if sorted {
		return key + auxIndexListSortedSuffix
	}
	return key + auxIndexListNonSortedSuffix
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
