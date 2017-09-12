package redis

import (
	"context"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
)

// Handler handles resource storage in Redis.
type Handler struct {
	client *redis.Client
	entityName string
}

// NewHandler creates a new redis handler
func NewHandler(client *redis.Client, entityName string) *Handler {
	return &Handler{
		client: client,
		entityName: entityName,
	}
}

// newRedisItem converts a resource.Item into a suitable for go-redis HMSet key and value
func (h *Handler) newRedisItem(i *resource.Item) (string, map[string]interface{}) {
	key := fmt.Sprintf("%s:%s", h.entityName, i.ID)

	// Filter out id from the payload so we don't store it twice
	value := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			value[k] = v
		}
	}
	value["etag"] = i.ETag
	value["updated"] = i.ETag

	return key, value
}

// Insert inserts new items in the Redis database
func (h *Handler) Insert(ctx context.Context, items []*resource.Item) error {
	pipe := h.client.Pipeline()

	for _, item := range items {
		key, value  := h.newRedisItem(item)
		pipe.HMSet(key, value)
	}
	//// Apply context deadline if any
	//if t := ctxTimeout(ctx); t != "" {
	//	bulk.Timeout(t)
	//}
	//
	//_, err := pipe.Exec()
	//
	//// Set the refresh flag to true if requested
	//bulk.Refresh(h.Refresh)
	//res, err := bulk.Do(ctx)
	//if err != nil {
	//	if !translateError(&err) {
	//		err = fmt.Errorf("insert error: %v", err)
	//	}
	//} else if res.Errors {
	//	for i, f := range res.Failed() {
	//		// CAVEAT on a bulk insert, if some items are in error, the
	//		// operation is not atomic and the request will partially succeed. I
	//		// don't see how to perform atomic bulk insert with ES.
	//		if isConflict(f.Error) {
	//			err = resource.ErrConflict
	//		} else {
	//			err = fmt.Errorf("insert error on item #%d: %#v", i+1, f.Error)
	//		}
	//		break
	//	}
	//}
	return error("j")
}

// Update replace an item by a new one in Redis
func (h Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	return error("j")
}

// Delete deletes an item from Redis
func (h Handler) Delete(ctx context.Context, item *resource.Item) error {
	return error("j")
}

// Clear clears all items from Redis matching the query
func (h Handler) Clear(ctx context.Context, q *query.Query) (int, error) {
	return 0, error("j")
}

// Find items from Redis matching the provided query
func (h Handler) Find(ctx context.Context, q *query.Query) (*resource.ItemList, error) {
	return &resource.ItemList{}, error("j")
}

// Count counts the number items matching the lookup filter
func (h Handler) Count(ctx context.Context, query *query.Query) (int, error) {
	return 0, error("j")
}
