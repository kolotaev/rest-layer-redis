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
	return fmt.Sprintf("tmp.%s.%d.%d.%d", q.entityName, getGoRoutineID(), rand.Int(), time.Now().UnixNano())
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
				redis.call('SADD', '%s', unpack(redis.call('ZRANGEBYSCORE', '%s', %d, %d)))
				`, key, zKey(q.entityName, t.Field), t.Value, t.Value)
			} else {
				b[key] = fmt.Sprintf(`
				redis.call('SADD', '%s', unpack(redis.call("SMEMBERS", '%s')))
				`, key, sKey(q.entityName, t.Field, t.Value))
			}
		case query.NotEqual:
			key := q.tmpKey()
			tempKeys = append(tempKeys, key)
			if isNumeric(t.Value) {
				b[key] = fmt.Sprintf(`
				redis.call('ZUNIONSTORE', '%s', 1, '%s')
				redis.call('ZREMRANGEBYSCORE', '%s', %d, %d)
				`, key, zKey(q.entityName, t.Field), key, t.Value, t.Value)
			} else {
				b[key] = fmt.Sprintf(`
				 redis.call('SDIFFSTORE', '%s', '%s', '%s')
				`, key, sIDsKey(q.entityName), sKey(q.entityName, t.Field, t.Value))
			}
		case query.GreaterThan:
			key := q.tmpKey()
			tempKeys = append(tempKeys, key)
			// todo: if zrange returns nil elements? the same for above
			// eval "redis.call('SADD', 'zset2-out-nil', unpack(redis.call('ZRANGEBYSCORE', 'zset2', 2000, '+inf')))" 0 0
			// ERR Error running script (call to f_9512e9c187ff6b9cfea6ac955a5dbc07eb6b964a):
			// @user_script:1: @user_script: 1: Wrong number of args calling Redis command From Lua script
			b[key] = fmt.Sprintf(`
				redis.call('SADD', '%s', unpack(redis.call('ZRANGEBYSCORE', '%s', '(%d', '+inf')))
				`, key, zKey(q.entityName, t.Field), t.Value)
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
