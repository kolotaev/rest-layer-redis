package rds

import (
	"encoding/json"
	"time"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
)

type ItemManager struct {
	EntityName string
	// TODO - not needed with json
	FieldNames []string
	// needed to determine what secondary indices we are going to create to allow filtering (see predicate.go).
	Filterable []string
	// needed for SORT type determination.
	Numeric    []string
	Sortable   []string
} 

// NewRedisItem converts a resource.Item into a suitable for go-redis HMSet [key, value] pair
func (im *ItemManager) NewRedisItem(i *resource.Item) (string, map[string]interface{}) {
	value := map[string]interface{}{}
	payload := map[string]interface{}{}

	for k, v := range i.Payload {
		if k != "id" {
			// Filter out id from the payload so we don't store it twice
			payload[k] = v
		}

		if inSlice(k, im.Sortable) {
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

	return im.RedisItemKey(i), value
}

// NewItem converts a Redis item from DB into resource.Item
func (im *ItemManager) NewItem(data []interface{}) *resource.Item {
	payload := make(map[string]interface{})
	item := new(resource.Item)

	for i, v := range im.FieldNames {
		value := data[i].(string)
		if v == payloadField {
			json.Unmarshal([]byte(value), &payload)
			item.Payload = payload
		} else if v == IDField {
			item.ID = value
		} else if v == ETagField {
			item.ETag = value
		} else if v == updatedField {
			item.Updated, _ = time.Parse(dateTimeFormat, value)
		}
	}

	// explicitly add ID and updated to a payload
	payload["id"] = item.ID
	payload["updated"] = item.Updated

	return item
}

// RedisItemKey returns a redis-compatible string key to denote a Hash key of an item. E.g. 'users:1234'.
func (im *ItemManager) RedisItemKey(i *resource.Item) string {
	return fmt.Sprintf("%s:%s", im.EntityName, i.ID)
}

// IndexSetKeys returns a secondary index keys for a resource's filterable fields suited for SET.
// Is used so that we can find them when needed.
// Ex: for user A returns ["users:hair:brown", "users:city:NYC"]
//     for user B returns ["users:hair:red", "users:city:Boston"]
func (im *ItemManager) IndexSetKeys(i *resource.Item) []string {
	var result []string
	for _, field := range im.Filterable {
		if value, ok := i.Payload[field]; ok && !isNumeric(value) {
			result = append(result, sKey(im.EntityName, field, value))
		}
	}
	// TODO - do we need etag? Isn't ID already in filterable?
	result = append(result, sKey(im.EntityName, "id", i.ID))
	return result
}

// IndexZSetKeys returns a secondary index keys for a resource's filterable fields suited for ZSET.
// Is used so that we can find them when needed.
// Ex: for user A returns {"users:age": 24, "users:salary": 75000}
//     for user B returns {"users:age": 56, "users:salary": 125000}
func (im *ItemManager) IndexZSetKeys(i *resource.Item) map[string]float64 {
	// TODO: float for all?
	result := make(map[string]float64)
	for _, field := range im.Filterable {
		if value, ok := i.Payload[field]; ok && isNumeric(value) {
			result[zKey(im.EntityName, field)] = valueToFloat(value)
		}
	}
	// TODO - do we need etag? Isn't updated already in filterable?
	result[zKey(im.EntityName, "updated")] = valueToFloat(i.Updated)

	return result
}

// AddSecondaryIndices adds:
// - new values to a secondary index for a given item.
// - index names to a maintained auxiliary list of item's indices.
// Action is appended to a Redis pipeline.
func (im *ItemManager) AddSecondaryIndices(pipe redis.Pipeliner, item *resource.Item) {
	var setIndexes, zSetIndexes []interface{}
	itemID := im.RedisItemKey(item)
	for _, v := range im.IndexSetKeys(item) {
		pipe.SAdd(v, itemID)
		setIndexes = append(setIndexes, v)
	}
	for k, v := range im.IndexZSetKeys(item) {
		pipe.ZAdd(k, redis.Z{Member: itemID, Score: v})
		zSetIndexes = append(zSetIndexes, k)
	}
	if len(setIndexes) > 0 {
		pipe.SAdd(auxIndexListKey(itemID, false), setIndexes...)
	}
	if len(zSetIndexes) > 0 {
		pipe.SAdd(auxIndexListKey(itemID, true), zSetIndexes...)
	}
}

// DeleteSecondaryIndices removes:
// - a secondary index for a given item.
// - index names to a maintained auxiliary list of item's indices.
// Action is appended to a Redis pipeline.
func (im *ItemManager) DeleteSecondaryIndices(pipe redis.Pipeliner, item *resource.Item) {
	var setIndexes, zSetIndexes []interface{}
	itemID := im.RedisItemKey(item)
	for _, v := range im.IndexSetKeys(item) {
		pipe.SRem(v, itemID)
		setIndexes = append(setIndexes, v)
	}
	for k := range im.IndexZSetKeys(item) {
		pipe.ZRem(k, itemID)
		zSetIndexes = append(zSetIndexes, k)
	}
	// TODO - shouldn't we delete the entire list?
	if len(setIndexes) > 0 {
		pipe.SRem(auxIndexListKey(itemID, false), setIndexes...)
	}
	if len(zSetIndexes) > 0 {
		pipe.SRem(auxIndexListKey(itemID, true), zSetIndexes...)
	}
}

// TODO - generalize to secondary idxs?
// AddIDToAllIDsSet adds item's ID to a set of all stored IDs
func (im *ItemManager) AddIDToAllIDsSet(pipe redis.Pipeliner, i *resource.Item) {
	pipe.SAdd(sKeyIDsAll(im.EntityName), im.RedisItemKey(i))
}

// DeleteIDFromAllIDsSet removes item's ID from a set of all stored IDs
func (im *ItemManager) DeleteIDFromAllIDsSet(pipe redis.Pipeliner, i *resource.Item) {
	pipe.SRem(sKeyIDsAll(im.EntityName), im.RedisItemKey(i))
}
