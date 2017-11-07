package rds

import (
	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
	"time"
	"math/rand"
	"fmt"
	"strings"
	"gopkg.in/mgo.v2/bson"
)

type Query struct {
	entityName string
}

// Determine if value is numeric.
// Numerics are all ints, floats, time values.
func isNumeric(v ...query.Value) bool {
	switch v[0].(type) {
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

func (q *Query) translatePredicate(predicate query.Predicate) (map[string]interface{}, error) {
	var tempKeys []string
	var b map[string]interface{}

	for _, exp := range predicate {
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
			key1 := q.tmpKey()
			key2 := q.tmpKey()
			key3 := q.tmpKey()
			var1 := q.tmpKey()
			var2 := q.tmpKey()
			tempKeys = append(tempKeys, key1, key2, key3)
			var inKeys []interface{}

			if isNumeric(t.Values) {
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				}
				b[key1] = fmt.Sprintf(`
				local %[1]s = %[2]s
				for x = %[1]s do
					local ys = redis.call('ZRANGEBYSCORE', '%[3]s', x, x)
					if next(ys) ~= nil then
						redis.call('SADD', '%[4]s', unpack(ys))
					end
				end
				`, var1, makeLuaTable(inKeys), zKey(q.entityName, t.Field), key1)
			} else {
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				}
				b[key3] = fmt.Sprintf(`
				local %[1]s = %[2]s
				if next(%[1]s) != nil then
					redis.call('SADD', '%[3]s', unpack(%[1]s))
				end
				local %[4]s = redis.call('KEYS', '%[5]s')
				if next(%[4]s) != nil then
					redis.call('SADD', '%[6]s', unpack(%[4]s))
				end
				redis.call('SINTERSTORE', '%[7]s', '%[3]s', '%[6]s')
				`, var1, makeLuaTable(inKeys), key1, var2, sKeyLastAll(q.entityName, t.Field), key2, key3)
			}
		case query.NotIn:
			key1 := q.tmpKey()
			key2 := q.tmpKey()
			key3 := q.tmpKey()
			var1 := q.tmpKey()
			var2 := q.tmpKey()
			tempKeys = append(tempKeys, key1, key2, key3)
			var inKeys []interface{}

			if isNumeric(t.Values) {
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				}
				tuples := getRangePairs(inKeys)
				b[key1] = fmt.Sprintf(`
				for x = %[1]s do
					local ys = redis.call('ZRANGEBYSCORE', '%[2]s', '(' .. x[1], '(' .. x[2])
					if next(ys) ~= nil then
						redis.call('SADD', '%[3]s', unpack(ys))
					end
				end
				`, makeLuaTable(tuples), zKey(q.entityName, t.Field), key1)
				//for _, v := range t.Values {
				//	inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				//}
				//vals := fmt.Sprintf("{" + strings.Repeat("%d,", len(inKeys)) + "}", inKeys)
				//b[key1] = fmt.Sprintf(`
				//local %[1]s = %[2]s
				//local %[3]s = redis.call('ZRANGE', '%[4]s', 0, -1, 'WITHSCORES')
				//for x = %[3]s do
				//	local ys = redis.call('ZRANGEBYSCORE', '%[4]s', x, x)
				//	for y = ys do
				//		table.insert(%[3]s, y)
				//	end
				//end
				//if next(%[3]s) ~= nil then
				//	redis.call('SADD', '%[5]s', unpack(%[3]s))
				//end
				//`, var1, vals, var2, zKey(q.entityName, t.Field), key1)
			} else {
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				}
				b[key3] = fmt.Sprintf(`
				local %[1]s = %[2]s
				if next(%[1]s) != nil then
					redis.call('SADD', '%[3]s', unpack(%[1]s))
				end
				local %[4]s = redis.call('KEYS', '%[5]s')
				if next(%[1]s) != nil then
					redis.call('SADD', '%[6]s', unpack(%[4]s))
				end
				redis.call('SDIFFSTORE', '%[7]s', '%[6]s', '%[3]s')
				`, var1, makeLuaTable(inKeys), key1, var2, sKeyLastAll(q.entityName, t.Field), key2, key3)
			}
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
				// todo: check if all keys are deleted?
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
				if (string.len(x) >= 1) then
					...
				end
				redis.call('SADD', '%s', unpack(redis.call('ZRANGEBYSCORE', '%s', '(%d', '+inf')))
				`, key, zKey(q.entityName, t.Field), t.Value)
		case query.GreaterOrEqual:
			key := q.tmpKey()
			tempKeys = append(tempKeys, key)
			b[key] = fmt.Sprintf(`
				redis.call('SADD', '%s', unpack(redis.call('ZRANGEBYSCORE', '%s', %d, '+inf')))
				`, key, zKey(q.entityName, t.Field), t.Value)
		case query.LowerThan:
			key := q.tmpKey()
			tempKeys = append(tempKeys, key)
			b[key] = fmt.Sprintf(`
				redis.call('SADD', '%s', unpack(redis.call('ZRANGEBYSCORE', '%s', '-inf', '(%d')))
				`, key, zKey(q.entityName, t.Field), t.Value)
		case query.LowerOrEqual:
			key := q.tmpKey()
			tempKeys = append(tempKeys, key)
			b[key] = fmt.Sprintf(`
				redis.call('SADD', '%s', unpack(redis.call('ZRANGEBYSCORE', '%s', '-inf', %d)))
				`, key, zKey(q.entityName, t.Field), t.Value)
		default:
			return nil, resource.ErrNotImplemented
		}
	}
	return b, nil
}

func getQuery(q *query.Query) (string, error) {
	return translatePredicate(q.Predicate)
}
