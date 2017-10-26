package rds

import (
	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
	"time"
	"math/rand"
	"fmt"
	"gopkg.in/mgo.v2/bson"
)

type Query struct {
	entityName string
}

// Determine if value is numeric.
// Numerics are all ints, floats, time values.
func isNumeric(v query.Value) bool {
	switch v.(type) {
	case int, float64, time.Time:
		return true
	default:
		return false
	}
}

// getField translates a schema field into a Redis field:
func getField(f string) string {
	if f == "id" {
		return "__id__"
	}
	return f
}

func (q *Query) tmpKey() string {
	return fmt.Sprintf("tmp.%s.%d.%d.%d", q.entityName, getGoRoutineID(), rand.Int(), time.Now())
}

func (q *Query) translatePredicate(q query.Predicate) (string, error) {
	var tempKeys []string
	ps := make([]string, 0)
	var b map[string]interface{}

	for _, exp := range q {
		switch t := exp.(type) {
		case query.And:
			s := []bson.M{}
			for _, subExp := range t {
				sb, err := q.translatePredicate(query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				s = append(s, sb)
			}
			b["$and"] = s
		case query.Or:
			s := []bson.M{}
			for _, subExp := range t {
				sb, err := q.translatePredicate(query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				s = append(s, sb)
			}
			b["$or"] = s
		case query.In:
			return nil, resource.ErrNotImplemented
		case query.NotIn:
			return nil, resource.ErrNotImplemented
		case query.Equal:
			key := q.tmpKey()
			tempKeys = append(tempKeys, key)
			if isNumeric(t.Value) {
				b[key] = fmt.Sprintf(`
				redis.call('SADD', %s, unpack(redis.call('ZRANGEBYSCORE', %s, %d, %d)))
				`, key, zKey(q.entityName, t.Field), t.Value, t.Value)
			} else {
				b[key] = fmt.Sprintf(`
				redis.call('SADD', %s, unpack(redis.call("SMEMBERS", %s)))
				`, key, sKey(q.entityName, t.Field, t.Value))
			}
		case query.NotEqual:
			b[getField(t.Field)] = bson.M{"$ne": t.Value}
		case query.GreaterThan:
			b[getField(t.Field)] = bson.M{"$gt": t.Value}
		case query.GreaterOrEqual:
			b[getField(t.Field)] = bson.M{"$gte": t.Value}
		case query.LowerThan:
			b[getField(t.Field)] = bson.M{"$lt": t.Value}
		case query.LowerOrEqual:
			b[getField(t.Field)] = bson.M{"$lte": t.Value}
		case query.Regex:
			nil, resource.ErrNotImplemented
		default:
			return nil, resource.ErrNotImplemented
		}
	}
	return b, nil
}

func getQuery(q *query.Query) (string, error) {
	return translatePredicate(q.Predicate)
}
