package redis

import (
	"context"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/resource"
	"gopkg.in/olivere/elastic.v5"
	"fmt"
)

// Handler handles resource storage in Redis.
type Handler struct {
	client *redis.Client
}

// NewHandler creates a new redis handler
func NewHandler(c *redis.Client) *Handler {
	return &Handler{
		client: c,
	}
}

// newRedisItem converts a resource.Item into a redisItem
func newRedisItem(i *resource.Item) *mongoItem {
	// Filter out id from the payload so we don't store it twice
	p := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			p[k] = v
		}
	}
	return &mongoItem{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Payload: p,
	}
}

// Insert inserts new items in the Redis database
func (h *Handler) Insert(ctx context.Context, items []*resource.Item) error {
	bulk := h.client.Bulk()
	for _, item := range items {
		id, ok := item.ID.(string)
		if !ok {
			return errors.New("non string IDs are not supported with ElasticSearch")
		}
		doc := buildDoc(item)
		req := elastic.NewBulkIndexRequest().OpType("create").Index(h.index).Type(h.typ).Id(id).Doc(doc)
		bulk.Add(req)
	}
	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		bulk.Timeout(t)
	}
	// Set the refresh flag to true if requested
	bulk.Refresh(h.Refresh)
	res, err := bulk.Do(ctx)
	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("insert error: %v", err)
		}
	} else if res.Errors {
		for i, f := range res.Failed() {
			// CAVEAT on a bulk insert, if some items are in error, the
			// operation is not atomic and the request will partially succeed. I
			// don't see how to perform atomic bulk insert with ES.
			if isConflict(f.Error) {
				err = resource.ErrConflict
			} else {
				err = fmt.Errorf("insert error on item #%d: %#v", i+1, f.Error)
			}
			break
		}
	}
	return err
}
