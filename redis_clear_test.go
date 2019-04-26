package rds_test

import (
	"time"

	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
)


func (s *RedisMainTestSuite) TestClear() {
	bob := &resource.Item{
		ID: "clea_id1",
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
		ID: "clea_id2",
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

	// test Bob is wiped away by clear
	q := &query.Query{
		Window: &query.Window{Limit: 100},
		Predicate: query.Predicate{&query.Equal{Field: "id", Value: "clea_id1"}},
	}
	res, err := s.handler.Clear(s.ctx, q)
	s.NoError(err)
	s.Equal(1, res)
	s.NotZero(s.client.DbSize().Val())

	// test Linda isn't touched
	q = &query.Query{
		Window: &query.Window{Limit: 100},
		Predicate: query.Predicate{&query.Equal{Field: "id", Value: "clea_id2"}},
	}
	resultLinda, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(1, resultLinda.Total)
	s.Len(resultLinda.Items, 1)
	s.Equal("Linda", resultLinda.Items[0].Payload["name"])

	// test no entries left and DB is totally empty when linda is wiped with clear
	//resFinal, err := s.handler.Clear(s.ctx, q)
	//s.NoError(err)
	//s.Equal(1, resFinal)
	//s.Zero(s.client.DbSize().Val())
}
