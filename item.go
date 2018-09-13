package rds

import (
	"fmt"
	"time"
	"encoding/json"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
)

// newRedisItem converts a resource.Item into a suitable for go-redis HMSet [key, value] pair
func (h *Handler) newRedisItem(i *resource.Item) (string, map[string]interface{}) {
	value := map[string]interface{}{}
	payload := map[string]interface{}{}

	for k, v := range i.Payload {
		if k != "id" {
			// Filter out id from the payload so we don't store it twice
			payload[k] = v
		}

		if inSlice(k, h.sortable) {
			if t, ok := v.(time.Time); ok {
				v = t.UnixNano()
			}
			value[k] = v
		}
	}

	value[IDField] = i.ID
	value[ETagField] = i.ETag
	// TODO we need em?
	value[updatedField] = i.Updated.Format(dateTimeFormat) // TODO -  Move to parser
	// TODO deal with _
	value[payloadField], _ = json.Marshal(payload)

	return h.redisItemKey(i), value
}

// newItem converts a Redis item from DB into resource.Item
func (h *Handler) newItem(data []interface{}) *resource.Item {
	payload := make(map[string]interface{})
	item := new(resource.Item)

	for i, v := range h.fieldNames {
		value := data[i].(string)
		if v == payloadField {
			json.Unmarshal([]byte(value), &payload)
			item.Payload = payload
			continue
		}
		if v == IDField {
			item.ID = value
		}
		if v == ETagField {
			item.ETag = value
		}
		if v == updatedField {
			item.Updated, _ = time.Parse(dateTimeFormat, value)
		}
	}

	// explicitly add ID tp a payload
	payload["id"] = item.ID

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
		// TODO - use semicolon here
		return key + auxIndexListSortedSuffix
	}
	return key + auxIndexListNonSortedSuffix
}

// TODO - generalise to secondary idxs?
// addIDToAllIDsSet adds item's ID to a set of all stored IDs
func (h *Handler) addIDToAllIDsSet(pipe redis.Pipeliner, i *resource.Item) {
	pipe.SAdd(sIDsKey(h.entityName), h.redisItemKey(i))
}

// deleteIDFromAllIDsSet removes item's ID from a set of all stored IDs
func (h *Handler) deleteIDFromAllIDsSet(pipe redis.Pipeliner, i *resource.Item) {
	pipe.SRem(sIDsKey(h.entityName), h.redisItemKey(i))
}
