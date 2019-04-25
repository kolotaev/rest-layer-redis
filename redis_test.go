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

// Make sure that Redis DB is clean after each test
// before each test
func (s *InsertTestSuite) TeardownTest() {
	s.client.FlushAll()
}


func (s *InsertTestSuite) TestInsert() {
	updated := time.Now()
	birth := time.Now()
	item := &resource.Item{
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
	}

	err := s.handler.Insert(s.ctx, []*resource.Item{item})
	s.NoError(err)

	// test we can fetch it back
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
		s.Equal(birth.Format(time.RFC3339Nano), val.Format(time.RFC3339Nano))
	} else {
		s.Fail(`Payload["birth"] is not of time.Time`)
	}
}


func (s *InsertTestSuite) TestDelete() {
	bob := &resource.Item{
		ID: "del_id1",
		ETag: "asdfq",
		Payload: map[string]interface{}{
			"age": 35,
			"birth": time.Now(),
			"height": 185.54576,
			"name": "Bob",
			"male": true,
		},
	}
	linda := &resource.Item{
		ID: "del_id2",
		ETag: "asdfq",
		Payload: map[string]interface{}{
			"age": 7,
			"birth": time.Now(),
			"height": 55,
			"name": "Linda",
		},
	}

	err := s.handler.Insert(s.ctx, []*resource.Item{bob, linda})
	s.NoError(err)


	// test no errors on deletion
	err = s.handler.Delete(s.ctx, bob)
	s.NoError(err)

	// test Bob is wiped away
	q := &query.Query{
		Window: &query.Window{Limit: 1},
		Predicate: query.Predicate{query.Equal{Field: "id", Value: "del_id1"}},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(0, res.Total)
	s.Len(res.Items, 0)
	s.NotZero(s.client.DbSize().Val())

	// test Linda isn't touched
	q = &query.Query{
		Window: &query.Window{Limit: 1},
		Predicate: query.Predicate{query.Equal{Field: "id", Value: "del_id2"}},
	}
	res, err = s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(1, res.Total)
	s.Len(res.Items, 1)
	s.Equal("Linda", res.Items[0].Payload["name"])

	// test no entries left and DB is totally empty
	err = s.handler.Delete(s.ctx, linda)
	s.NoError(err)
	s.Zero(s.client.DbSize().Val())
}


func (s *InsertTestSuite) TestDelete_Conflict() {
	bob := &resource.Item{
		ID: "del_id3",
		ETag: "asdf",
		Payload: map[string]interface{}{
			"age": 35,
			"birth": time.Now(),
			"height": 185.54576,
			"name": "Bob",
			"male": true,
		},
	}

	err := s.handler.Insert(s.ctx, []*resource.Item{bob})
	s.NoError(err)


	// test conflict error on deletion
	bob.ETag = "qwerty"
	err = s.handler.Delete(s.ctx, bob)
	s.EqualError(err, "Conflict")

	// test Bob is not wiped away
	q := &query.Query{
		Window: &query.Window{Limit: 1},
		Predicate: query.Predicate{query.Equal{Field: "id", Value: "del_id3"}},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(1, res.Total)
	s.Len(res.Items, 1)
	s.Equal("del_id3", res.Items[0].ID)
	s.Equal("asdf", res.Items[0].ETag)
	s.Equal("Bob", res.Items[0].Payload["name"])
}


func (s *InsertTestSuite) TestUpdate() {
	bob := &resource.Item{
		ID: "upd_id1",
		ETag: "asdf",
		Payload: map[string]interface{}{
			"age": 35,
			"birth": time.Now(),
			"height": 185.54576,
			"name": "Bob",
			"male": true,
		},
	}

	err := s.handler.Insert(s.ctx, []*resource.Item{bob})
	s.NoError(err)


	bobUpdated := &resource.Item{
		ID: "upd_id1",
		ETag: "asdf2",
		Payload: map[string]interface{}{
			"age": 77,
			"height": 186,
			"name": "Боб",
			"birth": time.Now(),
			"male": true,
		},
	}

	// test no errors on update
	err = s.handler.Update(s.ctx, bobUpdated, bob)
	s.NoError(err)

	// test we can fetch it back
	q := &query.Query{
		Window: &query.Window{
			Offset: 0,
			Limit: 1,
		},
		Predicate: query.Predicate{
			query.Equal{Field: "id", Value: "upd_id1"},
		},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Len(res.Items, 1)
	result := res.Items[0]

	// test Bob has new values and unchanged ones
	s.Equal("upd_id1", result.ID)
	s.Equal("asdf2", result.ETag)
	s.Len(result.Payload, 7)
	s.Equal(77, result.Payload["age"])
	s.Equal(186, result.Payload["height"])
	s.Equal("Боб", result.Payload["name"])
	s.Equal(true, result.Payload["male"])
	s.Equal("upd_id1", result.Payload["id"])
}


func (s *InsertTestSuite) TestUpdate_Conflict() {
	bob := &resource.Item{
		ID: "upd_id1",
		ETag: "asdf",
		Payload: map[string]interface{}{
			"age": 35,
			"birth": time.Now(),
			"height": 185.54576,
			"name": "Bob",
			"male": true,
		},
	}

	err := s.handler.Insert(s.ctx, []*resource.Item{bob})
	s.NoError(err)

	bobUpdated := &resource.Item{
		ID: "upd_id1",
		ETag: "asdf3",
		Payload: map[string]interface{}{
			"age": 77,
		},
	}
	bob.ETag = "asdf2"

	// test conflict error on update
	err = s.handler.Update(s.ctx, bobUpdated, bob)
	s.EqualError(err, "Conflict")

	// test we can fetch it back
	q := &query.Query{
		Window: &query.Window{
			Offset: 0,
			Limit: 1,
		},
		Predicate: query.Predicate{
			query.Equal{Field: "id", Value: "upd_id1"},
		},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Len(res.Items, 1)
	result := res.Items[0]

	// test Bob's data wasn't changed because of a conflict
	s.Equal("upd_id1", result.ID)
	s.Equal("asdf", result.ETag)
	s.Len(result.Payload, 7)
	s.Equal(35, result.Payload["age"])
	s.Equal(185.54576, result.Payload["height"])
	s.Equal("Bob", result.Payload["name"])
	s.Equal(true, result.Payload["male"])
	s.Equal("upd_id1", result.Payload["id"])
}
