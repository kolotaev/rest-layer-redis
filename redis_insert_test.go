package rds_test

import (
	"testing"
	"time"
	"context"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
	"github.com/stretchr/testify/suite"

	rds "github.com/kolotaev/rest-layer-redis"
)

const redisAddress = "127.0.0.1:6379"
const usersEntity = "users"

var userSchema = schema.Schema{
	Fields: schema.Fields{
		"id":      schema.IDField,
		"updated": schema.UpdatedField,
		"name": {
			Required:   true,
			Filterable: true,
			Sortable:   true,
			Validator: &schema.String{
				MaxLen: 150,
			},
		},
		"age": {
			Required:   true,
			Filterable: true,
			Sortable:   true,
			Validator: &schema.Integer{},
		},
		"birth": {
			Required:   true,
			Filterable: true,
			Sortable:   false,
			Validator: &schema.Time{},
		},
		"height": {
			Required:   true,
			Filterable: true,
			Sortable:   false,
			Validator: &schema.Float{},
		},
		"male": {
			Required:   true,
			Filterable: true,
			Sortable:   true,
			Default: false,
			Validator: &schema.Bool{},
		},
	},
}

type InsertTestSuite struct {
	suite.Suite

	client *redis.Client
	handler *rds.Handler
	ctx context.Context
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInsertSuite(t *testing.T) {
	suite.Run(t, new(InsertTestSuite))
}

func (s *InsertTestSuite) SetupSuite() {
	s.client = redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})

	_, err := s.client.Ping().Result()
	if err != nil {
		s.T().Fatal(err)
	}

	s.handler = rds.NewHandler(s.client, usersEntity, userSchema)
	s.ctx = context.Background()
}

// Make sure that Redis DB is clean before each test
// before each test
func (s *InsertTestSuite) SetupTest() {
	s.client.FlushAll()
}


func (s *InsertTestSuite) TestInsertOne() {
	updated := time.Now()
	birth := time.Now()
	items := []*resource.Item{
		{
			ID: "d4uhqvttith6uqnvrrq7",
			ETag: "asdf",
			Updated: updated,
			Payload: map[string]interface{}{
				"age": 35,
				"birth": birth,
				"height": 185.54576,
				"name": "Bob",
				"male": true,
			},
		},
	}

	err := s.handler.Insert(s.ctx, items)
	s.NoError(err)

	q := &query.Query{
		Window: &query.Window{
			Offset: 0,
			Limit: 1,
		},
		Predicate: query.Predicate{
			query.Equal{Field: "id", Value: "d4uhqvttith6uqnvrrq7"},
		},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)

	s.Len(res.Items, 1)
	result := res.Items[0]

	s.Equal("d4uhqvttith6uqnvrrq7", result.ID)
	s.Equal("asdf", result.ETag)
	s.Equal(updated.Format(time.RFC3339Nano), result.Updated.Format(time.RFC3339Nano))

	s.Len(result.Payload, 7)
	s.Equal(35, result.Payload["age"])
	s.Equal(185.54576, result.Payload["height"])
	s.Equal("Bob", result.Payload["name"])
	s.Equal(true, result.Payload["male"])
	s.Equal("d4uhqvttith6uqnvrrq7", result.Payload["id"])
	s.IsType(time.Time{}, result.Payload["updated"])
	if val, ok := result.Payload["updated"].(time.Time); ok {
		s.Equal(updated.Format(time.RFC3339Nano), val.Format(time.RFC3339Nano))
	} else {
		s.Fail(`Payload["updated"] is not of time.Time`)
	}
	s.IsType(time.Time{}, result.Payload["birth"])
	if val, ok := result.Payload["birth"].(time.Time); ok {
		s.Equal(updated.Format(time.RFC3339Nano), val.Format(time.RFC3339Nano))
	} else {
		s.Fail(`Payload["birth"] is not of time.Time`)
	}
}
