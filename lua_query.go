package rds

import (
	"fmt"
	"strings"
	"sort"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
)

// LuaQuery represents a result of building Redis select query as a Lua script
type LuaQuery struct {
	Script string
	LastKey string
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
		return nil, resource.ErrNotImplemented
	}

	resultVar := tmpVar()

	// Determine sort direction
	var sortByField, direction string
	sortByFieldRaw := q.Sort[0]
	if strings.HasPrefix(sortByFieldRaw, "-") {
		sortByField = sortByFieldRaw[1:len(sortByFieldRaw)-1]
		direction = "DESC"
	} else {
		sortByField = sortByFieldRaw
		direction = "ASC"
	}

	// First, we are sorting the set with all IDs
	lq.Script += fmt.Sprintf("\n local %s = redis.call('SORT', '%s', 'BY'", resultVar, lq.LastKey)

	// Add sorter field
	if sort.SearchStrings(numeric, sortByField) > 0 {
		lq.Script += fmt.Sprintf(", '*->%s', '%s'", lq.LastKey, direction)
	} else {
		lq.Script += fmt.Sprintf(", '*->%s', 'ALPHA', '%s'", lq.LastKey, direction)
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

	return lq, nil
}

func (lq *LuaQuery) addDelete() {
	resultVar := tmpVar()
	lq.Script += fmt.Sprintf("\n local %s = redis.call('SORT', '%s', 'BY'", resultVar, lq.LastKey)

	// Delete everything we've created previously
	lq.deleteTemporaryKeys()

	// Return the result
	lq.Script += fmt.Sprintf("\n return %s", resultVar)
}

func (lq *LuaQuery) deleteTemporaryKeys() {
	lq.Script = lq.Script + fmt.Sprintf("\nredis.call('DEL', unpack(%s))", makeLuaTableFromStrings(lq.AllKeys))
}
