package rds

import (
	"fmt"
	"strings"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
)

// getField translates a schema field into a Redis field:
// TODO: do we need it?
func getField(f string) string {
	if f == "id" {
		return "__id__"
	}
	return f
}

// normalizePredicate turns implicit AND on list of params of rest-layer query into an explicit AND-predicate
func normalizePredicate(predicate query.Predicate) query.Predicate {
	if len(predicate) > 1 {
		return query.Predicate{query.And{predicate}}
	}
	return predicate
}

// translatePredicate interprets rest-layer query to a Lua query script to be fed to Redis.
// This results in a Lua query that ultimately creates a Redis sorted-set with the IDs of the items corresponding
// to the initial query. Also you get a key in which this set is stored and a list a temporary keys
// you should delete later
func translatePredicate(entityName string, predicate query.Predicate) (string, string, []string, error) {
	var tempKeys []string
	newKey := func() string {
		k := tmpVar()
		tempKeys = append(tempKeys, k)
		return k
	}

	for _, exp := range predicate {
		switch t := exp.(type) {
		case query.And:
			var subs, keys []string
			var key string
			for _, subExp := range t {
				k, res, _, err := translatePredicate(entityName, query.Predicate{subExp})
				if err != nil {
					return "", "", nil, err
				}
				keys = append(keys, k)
				subs = append(subs, res)
			}
			if len(keys) > 1 {
				key = newKey()
				andClause := fmt.Sprintf(
					"redis.call('ZINTERSTORE', '%[1]s', unpack(%[2]s))",
					key, makeLuaTableFromStrings(keys))
				subs = append(subs, andClause)
			} else {
				// Nothing to intersect here - we have only one Set(ZSet)
				key = keys[len(keys)-1]
			}
			return key, strings.Join(subs, "\n"), tempKeys, nil
		case query.Or:
			var subs, keys []string
			var key string
			for _, subExp := range t {
				k, res, _, err := translatePredicate(entityName, query.Predicate{subExp})
				if err != nil {
					return "", "", nil, err
				}
				keys = append(keys, k)
				subs = append(subs, res)
			}
			if len(keys) > 1 {
				key = newKey()
				orClause := fmt.Sprintf(
					"redis.call('ZUNIONSTORE', '%[1]s', unpack(%[2]s))",
					key, makeLuaTableFromStrings(keys))
				subs = append(subs, orClause)
			} else {
				// Nothing to union here - we have only one Set(ZSet)
				key = keys[len(keys)-1]
			}
			return key, strings.Join(subs, "\n"), tempKeys, nil
		case query.In:
			key1 := newKey()
			key2 := newKey()
			key3 := newKey()
			var1 := tmpVar()
			var2 := tmpVar()

			if isNumeric(t.Values) {
				result := fmt.Sprintf(`
				local %[1]s = %[2]s
				for x = %[1]s do
					local ys = redis.call('ZRANGEBYSCORE', '%[3]s', x, x)
					if next(ys) ~= nil then
						redis.call('SADD', '%[4]s', unpack(ys))
					end
				end
				`, var1, makeLuaTableFromValues(t.Values), zKey(entityName, t.Field), key1)
				return key1, result, tempKeys, nil
			} else {
				var inKeys []string
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(entityName, t.Field, v))
				}
				// todo: ew don't need local local %[1]s = %[2]s - just inline!
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
				`, var1, makeLuaTableFromStrings(inKeys), key1, var2, sKeyLastAll(entityName, t.Field), key2, key3)
				return key3, result, tempKeys, nil
			}
		case query.NotIn:
			key1 := newKey()
			key2 := newKey()
			key3 := newKey()

			if isNumeric(t.Values) {
				result := fmt.Sprintf(`
				for x = %[1]s do
					local ys = redis.call('ZRANGEBYSCORE', '%[2]s', '(' .. x[1], '(' .. x[2])
					if next(ys) ~= nil then
						redis.call('SADD', '%[3]s', unpack(ys))
					end
				end
				`, makeLuaTableFromStrings(getRangePairs(t.Values)), zKey(entityName, t.Field), key1)
				return key1, result, tempKeys, nil
			} else {
				var inKeys []string
				var1 := tmpVar()
				var2 := tmpVar()
				for _, v := range t.Values {
					inKeys = append(inKeys, sKey(entityName, t.Field, v))
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
				`, var1, makeLuaTableFromStrings(inKeys), key1, var2, sKeyLastAll(entityName, t.Field), key2, key3)
				return key3, result, tempKeys, nil
			}
		case query.Equal:
			var result string
			key := newKey()
			if isNumeric(t.Value) {
				result = fmt.Sprintf(`
				local %[5]s = redis.call('ZRANGEBYSCORE', '%[2]s', %[3]d, %[4]d)
				if next(%[5]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[5]s))
				end
				`, key, zKey(entityName, t.Field), t.Value, t.Value, tmpVar())
			} else {
				result = fmt.Sprintf(`
				local %[3]s = redis.call('SMEMBERS', '%[2]s')
				if next(%[3]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[3]s))
				end
				`, key, sKey(entityName, t.Field, t.Value), tmpVar())
			}
			return key, result, tempKeys, nil
		case query.NotEqual:
			var result string
			key := newKey()
			if isNumeric(t.Value) {
				result = fmt.Sprintf(`
				redis.call('ZUNIONSTORE', '%s', 1, '%s')
				redis.call('ZREMRANGEBYSCORE', '%s', %d, %d)
				`, key, zKey(entityName, t.Field), key, t.Value, t.Value)
			} else {
				result = fmt.Sprintf(`
				 redis.call('SDIFFSTORE', '%s', '%s', '%s')
				`, key, sIDsKey(entityName), sKey(entityName, t.Field, t.Value))
			}
			return key, result, tempKeys, nil
		case query.GreaterThan:
			key := newKey()
			result := fmt.Sprintf(`
				local %[4]s = redis.call('ZRANGEBYSCORE', '%[2]s', '(%[3]s', '+inf')
				if next(%[4]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[4]s))
				end
				`, key, zKey(entityName, t.Field), t.Value, tmpVar())
			return key, result, tempKeys, nil
		case query.GreaterOrEqual:
			key := newKey()
			result := fmt.Sprintf(`
				local %[4]s = redis.call('ZRANGEBYSCORE', '%[2]s', %[3]d, '+inf')
				if next(%[4]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[4]s))
				end
				`, key, zKey(entityName, t.Field), t.Value, tmpVar())
			return key, result, tempKeys, nil
		case query.LowerThan:
			key := newKey()
			result := fmt.Sprintf(`
				local %[4]s = redis.call('ZRANGEBYSCORE', '%[2]s', '-inf', '(%[3]s')
				if next(%[4]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[4]s))
				end
				`, key, zKey(entityName, t.Field), t.Value, tmpVar())
			return key, result, tempKeys, nil
		case query.LowerOrEqual:
			key := newKey()
			tempKeys = append(tempKeys, key)
			result := fmt.Sprintf(`
				local %[4]s = redis.call('ZRANGEBYSCORE', '%[2]s', '-inf', %[3]d)
				if next(%[4]s) != nil then
					redis.call('SADD', '%[1]s', unpack(%[4]s))
				end
				`, key, zKey(entityName, t.Field), t.Value, tmpVar())
			return key, result, tempKeys, nil
		default:
			return "", "", nil, resource.ErrNotImplemented
		}
	}
	return "", "", tempKeys, nil
}
