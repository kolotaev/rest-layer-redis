package rds_test

import (
	"time"

	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
)


func (s *RedisMainTestSuite) TestDelete() {
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
		Predicate: query.Predicate{&query.Equal{Field: "id", Value: "del_id1"}},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(0, res.Total)
	s.Len(res.Items, 0)
	s.NotZero(s.client.DbSize().Val())

	// test Linda isn't touched
	q = &query.Query{
		Window: &query.Window{Limit: 100},
		Predicate: query.Predicate{&query.Equal{Field: "id", Value: "del_id2"}},
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


func (s *RedisMainTestSuite) TestDelete_Conflict() {
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
		Window: &query.Window{Limit: 100},
		Predicate: query.Predicate{&query.Equal{Field: "id", Value: "del_id3"}},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(1, res.Total)
	s.Len(res.Items, 1)
	s.Equal("del_id3", res.Items[0].ID)
	s.Equal("asdf", res.Items[0].ETag)
	s.Equal("Bob", res.Items[0].Payload["name"])
}
