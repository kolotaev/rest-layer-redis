package rds

import (
	"time"
	"fmt"

	"github.com/rs/rest-layer/schema/query"
	"github.com/rs/rest-layer/resource"
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


// Turns implicit AND of a list of params into an explicit AND-predicate
func normalizePredicate(predicate query.Predicate) query.Predicate {
	if len(predicate) > 1 {
		return query.Predicate{query.And{predicate}}
	}
	return predicate
}

func (q *Query) translatePredicate(predicate query.Predicate) (string, string, []string, error) {
	var tempKeys []string

	for _, exp := range predicate {
		switch t := exp.(type) {
		case query.And:
			//var subs map[string]string
			//for _, subExp := range t {
			//	s, err := q.translatePredicate(query.Predicate{subExp})
			//	if err != nil {
			//		return nil, err
			//	}
			//	subs = append(subs, s)
			//}
			//key := tmpVar()
			//tempKeys = append(tempKeys, key)
			//b[key] =
		case query.Or:
			//s := []bson.M{}
			//for _, subExp := range t {
			//	sb, err := q.translatePredicate(query.Predicate{subExp})
			//	if err != nil {
			//		return nil, err
			//	}
			//	s = append(s, sb)
			//}
			//b["$or"] = s
		case query.In:
			key1 := tmpVar()
			key2 := tmpVar()
			key3 := tmpVar()
			var1 := tmpVar()
			var2 := tmpVar()
			tempKeys = append(tempKeys, key1, key2, key3)
			var inKeys []interface{}

			if isNumeric(t.Values) {
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				}
				result := fmt.Sprintf(`
				local %[1]s = %[2]s
				for x = %[1]s do
					local ys = redis.call('ZRANGEBYSCORE', '%[3]s', x, x)
					if next(ys) ~= nil then
						redis.call('SADD', '%[4]s', unpack(ys))
					end
				end
				`, var1, makeLuaTable(inKeys), zKey(q.entityName, t.Field), key1)
				return key1, result, tempKeys, nil
			} else {
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				}
				result := fmt.Sprintf(`
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
				return key3, result, tempKeys, nil
			}
		case query.NotIn:
			key1 := tmpVar()
			key2 := tmpVar()
			key3 := tmpVar()
			var1 := tmpVar()
			var2 := tmpVar()
			tempKeys = append(tempKeys, key1, key2, key3)
			var inKeys []interface{}

			if isNumeric(t.Values) {
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				}
				tuples := getRangePairs(inKeys)
				result := fmt.Sprintf(`
				for x = %[1]s do
					local ys = redis.call('ZRANGEBYSCORE', '%[2]s', '(' .. x[1], '(' .. x[2])
					if next(ys) ~= nil then
						redis.call('SADD', '%[3]s', unpack(ys))
					end
				end
				`, makeLuaTable(tuples), zKey(q.entityName, t.Field), key1)
				return key1, result, tempKeys, nil
			} else {
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(q.entityName, t.Field, v))
				}
				result := fmt.Sprintf(`
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
				return key3, result, tempKeys, nil
			}
		case query.Equal:
			var result string
			key := tmpVar()
			var1 := tmpVar()
			tempKeys = append(tempKeys, key)
			if isNumeric(t.Value) {
				result = fmt.Sprintf(`
				local %[5]s = redis.call('ZRANGEBYSCORE', '%[2]s', %[3]d, %[4]d)
				if next(%[5]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[5]s))
				end
				`, key, zKey(q.entityName, t.Field), t.Value, t.Value, var1)
			} else {
				result = fmt.Sprintf(`
				local %[3]s = redis.call('SMEMBERS', '%[2]s')
				if next(%[3]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[3]s))
				end
				`, key, sKey(q.entityName, t.Field, t.Value), var1)
			}
			return key, result, tempKeys, nil
		case query.NotEqual:
			var result string
			key := tmpVar()
			tempKeys = append(tempKeys, key)
			if isNumeric(t.Value) {
				// TODO: check if all keys are deleted?
				result = fmt.Sprintf(`
				redis.call('ZUNIONSTORE', '%s', 1, '%s')
				redis.call('ZREMRANGEBYSCORE', '%s', %d, %d)
				`, key, zKey(q.entityName, t.Field), key, t.Value, t.Value)
			} else {
				result = fmt.Sprintf(`
				 redis.call('SDIFFSTORE', '%s', '%s', '%s')
				`, key, sIDsKey(q.entityName), sKey(q.entityName, t.Field, t.Value))
			}
			return key, result, tempKeys, nil
		case query.GreaterThan:
			key := tmpVar()
			var1 := tmpVar()
			tempKeys = append(tempKeys, key)
			result := fmt.Sprintf(`
				local %[4]s = redis.call('ZRANGEBYSCORE', '%[2]s', '(%[3]s', '+inf')
				if next(%[4]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[4]s))
				end
				`, key, zKey(q.entityName, t.Field), t.Value, var1)
			return key, result, tempKeys, nil
		case query.GreaterOrEqual:
			key := tmpVar()
			var1 := tmpVar()
			tempKeys = append(tempKeys, key)
			result := fmt.Sprintf(`
				local %[4]s = redis.call('ZRANGEBYSCORE', '%[2]s', %[3]d, '+inf')
				if next(%[4]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[4]s))
				end
				`, key, zKey(q.entityName, t.Field), t.Value, var1)
			return key, result, tempKeys, nil
		case query.LowerThan:
			key := tmpVar()
			var1 := tmpVar()
			tempKeys = append(tempKeys, key)
			result := fmt.Sprintf(`
				local %[4]s = redis.call('ZRANGEBYSCORE', '%[2]s', '-inf', '(%[3]s')
				if next(%[4]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[4]s))
				end
				`, key, zKey(q.entityName, t.Field), t.Value, var1)
			return key, result, tempKeys, nil
		case query.LowerOrEqual:
			key := tmpVar()
			var1 := tmpVar()
			tempKeys = append(tempKeys, key)
			result := fmt.Sprintf(`
				local %[4]s = redis.call('ZRANGEBYSCORE', '%[2]s', '-inf', %[3]d)
				if next(%[4]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[4]s))
				end
				`, key, zKey(q.entityName, t.Field), t.Value, var1)
			return key, result, tempKeys, nil
		default:
			return "", "", nil, resource.ErrNotImplemented
		}
	}
	return "", "", tempKeys, nil
}

func (q *Query) getQuery(qu *query.Query) (string, error) {
	lastKey, qry, tempKeys, err := q.translatePredicate(normalizePredicate(qu.Predicate))
	qry += fmt.Sprintf("\nredis.call('DEL', unpack(%s))", makeLuaTable(tempKeys))
	return query, err
}
