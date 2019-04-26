package rds_test

import (
	"time"

	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
)


func (s *RedisMainTestSuite) TestUpdate() {
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
			Limit: 100,
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


func (s *RedisMainTestSuite) TestUpdate_Conflict() {
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
			Limit: 100,
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
