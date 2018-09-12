package rds

import (
	"fmt"
	"github.com/rs/rest-layer/schema/query"
	"math/rand"
	"sort"
	"strings"
	"time"
)

// Determine if value is numeric.
// Numeric values are all ints, floats, time values.
func isNumeric(v ...query.Value) bool {
	switch v[0].(type) {
	case int, float64, time.Time:
		return true
	default:
		return false
	}
}

// TODO - better?
func valueToFloat(v query.Value) float64 {
	if x, ok := interface{}(v).(int); ok {
		return float64(x)
	}
	if x, ok := interface{}(v).(time.Time); ok {
		return float64(x.Nanosecond())
	}
	if x, ok := interface{}(v).(float64); ok {
		return x
	}
	return -1.0
}

// getRangePairs creates consequent combinations of ASC-sorted input elements.
// Is used for creating range tuples for Lua.
// Ex: [a, c, d, b, e] -> ["{-inf,a}", "{a,b}", "{c,d}", ... "{e,+inf}"]
func getRangePairs(in []query.Value) []string {
	var strIn []string
	for _, i := range in {
		strIn = append(strIn, fmt.Sprintf("%v", i))
	}
	sort.Strings(strIn)
	strIn = append(strIn, "+inf")
	strIn = append([]string{"-inf"}, strIn...)

	var out []string
	for i := 1; i < len(strIn); i++ {
		var tuple = fmt.Sprintf("{'%s','%s'},", strIn[i-1], strIn[i])
		out = append(out, tuple)
	}
	return out
}

// Get a Lua table definition based on given values.
func makeLuaTableFromStrings(a []string) string {
	aQuoted := make([]string, 0, len(a))
	for _, v := range a {
		aQuoted = append(aQuoted, fmt.Sprintf("'%s'", v))
	}
	return fmt.Sprintf("{"+strings.Join(aQuoted, ",")+"}")
}

// Get a Lua table definition based on given values.
func makeLuaTableFromValues(a []query.Value) string {
	aQuoted := make([]string, 0, len(a))
	for _, v := range a {
		aQuoted = append(aQuoted, fmt.Sprintf("'%v'", v))
	}
	return fmt.Sprintf("{" + strings.Join(aQuoted, ",") + "}")
}

// Generate random string suited for temporary Lua variable and Redis key
func tmpVar() string {
	return fmt.Sprintf("tmp_%d_%d", rand.Int(), time.Now().UnixNano())
}

// Get key name for a Redis set.
// Ex: users:hair-color:brown
func sKey(entity, key string, value interface{}) string {
	return fmt.Sprintf("%s:%s:%v", entity, key, value)
}

// Get key name for a Redis sorted set.
// Ex: users:age
func zKey(entity, key string) string {
	return fmt.Sprintf("%s:%s", entity, key)
}

// Get a search pattern for the last element of a compound key (for Redis set).
// Ex: users:hair-color:* -> get all stored ages of users.
func sKeyLastAll(entity, key string) string {
	return fmt.Sprintf("%s:%s:*", entity, key)
}

// Get an IDs key name for set of all entity IDs.
// Ex: users:ids
func sIDsKey(entity string) string {
	return fmt.Sprintf("%s:ids", entity)
}
