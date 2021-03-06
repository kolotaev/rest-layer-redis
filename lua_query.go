package rds

import (
	"fmt"
	"sort"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
)

// LuaQuery represents a result of building Redis select query as a Lua script
type LuaQuery struct {
	// Script that will be executed on Redis Lua engine
	Script string
	// LastKey is the key where the ids against which the final query will be executed.
	LastKey string
	// AllKeys are temporary keys created in Redis during Query building process.
	// They should be eventually deleted after query returned some result.
	AllKeys []string
}

func (lq *LuaQuery) addSelect(entityName string, q *query.Query) error {
	lastKey, script, tempKeys, err := translatePredicate(entityName, normalizePredicate(q.Predicate))
	lq.Script = script
	lq.LastKey = lastKey
	lq.AllKeys = tempKeys
	return err
}

func (lq *LuaQuery) addSortWithLimit(q *query.Query, limit, offset int, fields, numeric []string) error {
	// Redis supports only one sort field.
	if len(q.Sort) > 1 {
		// todo - ErrNotImplemented ???
		return resource.ErrNotImplemented
	}

	sortByField := "__nosort__"
	direction := "ASC"
	resultVar := tmpVar()

	// todo - range q.sort - in order to sort by multiple
	// If sort is set, it' means we definitely use some real field, not a "nosort"
	// Determine sort direction and sort field
	if len(q.Sort) != 0 {
		sortByField = "*->" + q.Sort[0].Name
		if q.Sort[0].Reversed {
			direction = "DESC"
		}
	}

	// First, we are sorting the set with all IDs
	lq.Script += fmt.Sprintf("\n local %s = redis.call('SORT', '%s', 'BY'", resultVar, lq.LastKey)

	// Add sorter field
	// TODO - inSlice
	if sort.SearchStrings(numeric, sortByField) > 0 {
		lq.Script += fmt.Sprintf(", '%s', '%s'", sortByField, direction)
	} else {
		lq.Script += fmt.Sprintf(", '%s', 'ALPHA', '%s'", sortByField, direction)
	}

	// Add all fields to a result of a sort
	for _, v := range fields {
		lq.Script += fmt.Sprintf(", 'GET', '*->%s'", v)
	}

	// Add limit and offset
	lq.Script += fmt.Sprintf(", 'LIMIT', %d, %d)", offset, limit)

	// Delete everything we've created previously
	lq.deleteTemporaryKeys()

	// Return the result
	lq.Script += fmt.Sprintf("\n return %s", resultVar)
	return nil
}

func (lq *LuaQuery) addDelete(entityName string) {
	resultVar := tmpVar()

	// Delete all the entities we were asked to delete.
	// Also delete all the secondary indices (and auxiliary lists) for those entities.
	// Get and return the count of records that are going to be deleted.
	lq.Script += fmt.Sprintf(`
		local %[5]s
		local %[1]s
		if redis.call('TYPE', '%[2]s') == 'zset' then
			%[5]s = redis.call('ZCARD', '%[2]s')
			%[1]s = redis.call('ZRANGE', '%[2]s', 0, -1)
		else
			-- If not zset then it's a set
			%[5]s = redis.call('SCARD', '%[2]s')
			%[1]s = redis.call('SMEMBERS', '%[2]s')
		end

		for _, v in ipairs(%[1]s) do
			-- delete the item itself
			redis.call('DEL', v)

			-- delete secondary ZSet indices
			local idx_sorted_name = v .. '%[3]s'
			local idx_sorted = redis.call('SMEMBERS', idx_sorted_name)
			for _, i in ipairs(idx_sorted) do
				redis.call('ZREM', i, v)
			end
			-- delete auxiliary list of zset (sorted values) indices
			redis.call('DEL', idx_sorted_name)

			-- delete secondary Set indices
			local idx_non_sorted_name = v .. '%[4]s'
			local idx_non_sorted = redis.call('SMEMBERS', idx_non_sorted_name)
			for _, i in ipairs(idx_non_sorted) do
				redis.call('SREM', i, v)
			end
			-- delete auxiliary list of set (non-sorted values) indices
			redis.call('DEL', idx_non_sorted_name)

			-- delete item from all IDs set
			redis.call('SREM', '%[6]s', v)
		end
		`,
		tmpVar(),
		lq.LastKey,
		auxIndexListSortedSuffix,
		auxIndexListNonSortedSuffix,
		resultVar,
		sKeyIDsAll(entityName))

	// Delete everything we've created previously
	lq.deleteTemporaryKeys()

	// Return the result
	lq.Script += fmt.Sprintf("\n return %s", resultVar)
}

func (lq *LuaQuery) deleteTemporaryKeys() {
	// Add the main set we used to obtain result to keys marked-for-deletion
	// todo - isn't it too early?
	//lq.AllKeys = append(lq.AllKeys, lq.LastKey)
	if len(lq.AllKeys) > 0 {
		lq.Script = lq.Script + fmt.Sprintf("\n redis.call('DEL', unpack(%s))", makeLuaTableFromStrings(lq.AllKeys))
	}
}
