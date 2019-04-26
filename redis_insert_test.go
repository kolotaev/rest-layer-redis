package rds_test

import (
	"time"

	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
)

func (s *RedisMainTestSuite) TestInsert() {
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
			Limit: 100,
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


func (s *RedisMainTestSuite) TestInsert_Duplicates() {
	bob := &resource.Item{
		ID: "ins_id3",
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


	// test conflict error on duplicate insert
	err = s.handler.Insert(s.ctx, []*resource.Item{bob})
	s.EqualError(err, "Conflict")

	// test Bob is here and is single
	q := &query.Query{
		Window: &query.Window{Limit: 100},
		Predicate: query.Predicate{query.Equal{Field: "id", Value: "ins_id3"}},
	}
	res, err := s.handler.Find(s.ctx, q)
	s.NoError(err)
	s.Equal(1, res.Total)
	s.Len(res.Items, 1)
	s.Equal("ins_id3", res.Items[0].ID)
	s.Equal("asdf", res.Items[0].ETag)
	s.Equal("Bob", res.Items[0].Payload["name"])
}
