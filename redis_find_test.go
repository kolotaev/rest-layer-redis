package rds_test

import (
	"time"
	"fmt"

	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
)

var location, _ = time.LoadLocation("Local")

func getPersons() []*resource.Item {
	bob := &resource.Item{
		ID: "find_id1",
		ETag: "asdf",
		Payload: map[string]interface{}{
			"age": 19,
			"birth": time.Date(1990, time.April, 1, 1, 32, 59, 789, location),
			"height": 155.3,
			"name": "Bob",
			"male": true,
		},
	}
	linda := &resource.Item{
		ID: "find_id2",
		ETag: "asdfq",
		Payload: map[string]interface{}{
			"age": 7,
			"birth": time.Date(2019, time.July, 10, 8, 32, 59, 8, location),
			"height": 56.8,
			"name": "Linda",
		},
	}
	jim := &resource.Item{
		ID: "find_id3",
		ETag: "asdfq",
		Payload: map[string]interface{}{
			"age": 19,
			"birth": time.Date(2123, time.December, 28, 6, 6, 34, 899, location),
			"height": 155,
			"male": true,
			"name": "Jimmy",
		},
	}
	return []*resource.Item{bob, linda, jim}
}

func (s *RedisMainTestSuite) TestFind_LimitAndOffset() {
	err := s.handler.Insert(s.ctx, getPersons())
	s.NoError(err)

	cases := []struct {
		limit int
		offset  int
		expect int
	}{
		{0, 0, 0},
		{0, 1, 0},
		{1, 0, 1},
		{1, 1, 1},
		{1, 2, 0},
		{1, 3, 0},
		{2, 0, 2},
		{2, 1, 1},
		{2, 2, 0},
		{2, 3, 0},
		{3, 0, 2},
		{3, 1, 1},
		{3, 2, 0},
		{3, 3, 0},
		{4, 0, 2},
		{4, 1, 1},
		{4, 2, 0},
		{4, 3, 0},
		// todo - very large numbers?
		{1000000, 0, 2},
		{1000000, 1, 1},
		{1000000, 2, 0},
		{1000000, 1000000, 0},
	}
	for i, tc := range cases {
		msg := fmt.Sprintf("Test case #%d", i)
		q := &query.Query{
			Window:    &query.Window{Limit: tc.limit, Offset: tc.offset},
			Predicate: query.Predicate{&query.Equal{Field: "age", Value: 19}},
		}
		res, err := s.handler.Find(s.ctx, q)
		s.NoError(err, msg)
		s.Equal(tc.expect, res.Total, msg)
		s.Len(res.Items, tc.expect, msg)
	}
}


func (s *RedisMainTestSuite) TestFind_Equal() {
	err := s.handler.Insert(s.ctx, getPersons())
	s.NoError(err)

	// test can find by integer
	q := &query.Query{
		Window:    &query.Window{Limit: -1},
		Predicate: query.Predicate{&query.Equal{Field: "age", Value: 19}},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(2, res.Total)
	s.Len(res.Items, 2)
	s.Equal("find_id1", res.Items[0].ID)
	s.Equal(19, res.Items[0].Payload["age"])
	s.Equal("Bob", res.Items[0].Payload["name"])
	s.Equal("find_id3", res.Items[1].ID)
	s.Equal(19, res.Items[1].Payload["age"])
	s.Equal("Jimmy", res.Items[1].Payload["name"])

	// test can find by float
	q = &query.Query{
		Window:    &query.Window{Limit: -1},
		Predicate: query.Predicate{&query.Equal{Field: "height", Value: 56.8}},
	}
	res, err = s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(1, res.Total)
	s.Len(res.Items, 1)
	s.Equal("find_id2", res.Items[0].ID)
	s.Equal("Linda", res.Items[0].Payload["name"])
	s.Equal(56.8, res.Items[0].Payload["height"])

	// test can find by string
	q = &query.Query{
		Window:    &query.Window{Limit: -1},
		Predicate: query.Predicate{&query.Equal{Field: "name", Value: "Linda"}},
	}
	res, err = s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(1, res.Total)
	s.Len(res.Items, 1)
	s.Equal("find_id2", res.Items[0].ID)
	s.Equal("Linda", res.Items[0].Payload["name"])

	// test can find by date
	q = &query.Query{
		Window:    &query.Window{Limit: -1},
		Predicate: query.Predicate{
			&query.Equal{
				Field: "birth",
				Value: time.Date(2019, time.July, 10, 8, 32, 59, 8, location),
			},

		},
	}
	res, err = s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(1, res.Total)
	s.Len(res.Items, 1)
	s.Equal("find_id2", res.Items[0].ID)
	s.Equal("Linda", res.Items[0].Payload["name"])
}

func (s *RedisMainTestSuite) TestFind_NotEqual() {
	err := s.handler.Insert(s.ctx, getPersons())
	s.NoError(err)

	// test can find by integer
	q := &query.Query{
		Window:    &query.Window{Limit: -1},
		Predicate: query.Predicate{&query.NotEqual{Field: "age", Value: 7}},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(2, res.Total)
	s.Len(res.Items, 2)
	s.Equal("find_id1", res.Items[0].ID)
	s.Equal(19, res.Items[0].Payload["age"])
	s.Equal("Bob", res.Items[0].Payload["name"])
	s.Equal("find_id3", res.Items[1].ID)
	s.Equal(19, res.Items[1].Payload["age"])
	s.Equal("Jimmy", res.Items[1].Payload["name"])

	// test can find by float
	q = &query.Query{
		Window:    &query.Window{Limit: -1},
		Predicate: query.Predicate{&query.NotEqual{Field: "height", Value: 56.8}},
	}
	res, err = s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(2, res.Total)
	s.Len(res.Items, 2)
	s.Equal("find_id3", res.Items[0].ID)
	s.Equal(155, res.Items[0].Payload["height"])
	s.Equal("Jimmy", res.Items[0].Payload["name"])
	s.Equal("find_id1", res.Items[1].ID)
	s.Equal(155.3, res.Items[1].Payload["height"])
	s.Equal("Bob", res.Items[1].Payload["name"])


	// test can find by string
	q = &query.Query{
		Window:    &query.Window{Limit: -1},
		Predicate: query.Predicate{&query.NotEqual{Field: "name", Value: "Jimmy"}},
	}
	res, err = s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(2, res.Total)
	s.Len(res.Items, 2)
	s.Equal("find_id1", res.Items[0].ID)
	s.Equal("Bob", res.Items[0].Payload["name"])
	s.Equal("find_id2", res.Items[1].ID)
	s.Equal("Linda", res.Items[1].Payload["name"])
}
