package rds

import (
	"sort"
	"fmt"
	"strings"
	"time"
	"math/rand"
)

// makePairs creates consequent combinations of sorted input elements.
// Is used for creating range tuples for Lua.
// Ex: [a, c, d, b, e] -> ["{-inf,a}", "{a,b}", "{c,d}", ... "{e,+inf}"]
func getRangePairs(in []interface{}) []interface{} {
	var strIn []string
	for _, i := range in {
		strIn = append(strIn, string(i))
	}
	sort.Strings(strIn)
	strIn = append(strIn, "+inf")
	strIn = append([]string{"-inf"}, strIn...)

	var out []interface{}
	for i := 1; i < len(strIn); i++ {
		var tuple = fmt.Sprintf("{'%s','%s'},", strIn[i-1], strIn[i])
		out = append(out, tuple)
	}
	return out
}

// Get a Lua table definition based on given values.
func makeLuaTable(a []interface{}) string {
	return fmt.Sprintf("{" + strings.Repeat("'%s',", len(a)) + "}", a)
}

// Generate random string suited for temporary Lua variable and Redis key
func tmpVar() string {
	return fmt.Sprintf("tmp_%d_%d", rand.Int(), time.Now().UnixNano())
}

func zKey(entity, key string) string {
	return fmt.Sprintf("%s:%s", entity, key)
}

func sKey(entity, key string, value interface{}) string {
	return fmt.Sprintf("%s:%s:%s", entity, key, value)
}

func sKeyLastAll(entity, key string) string {
	return fmt.Sprintf("%s:%s:*", entity, key)
}

func sIDsKey(entity string) string {
	return fmt.Sprintf("%s:ids", entity)
}
