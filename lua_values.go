package rds

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/rs/rest-layer/schema/query"
)

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
		var tuple = fmt.Sprintf("{'%s','%s'},", strIn[i - 1], strIn[i])
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
	return fmt.Sprintf("{" + strings.Join(aQuoted, ",") + "}")
}

// Get a Lua table definition based on given values.
func makeLuaTableFromValues(a []query.Value) string {
	var finalVal string
	aQuoted := make([]string, 0, len(a))
	for _, v := range a {
		if val, ok := interface{}(v).(string); ok {
			finalVal = fmt.Sprintf("'%s'", val)
		} else {
			finalVal = fmt.Sprintf("%v", val)
		}
		aQuoted = append(aQuoted, finalVal)
	}
	return fmt.Sprintf("{" + strings.Join(aQuoted, ",") + "}")
}

// Generate random string suited for temporary Lua variable and Redis key
func tmpVar() string {
	return fmt.Sprintf("tmp_%d_%d", rand.Int(), time.Now().UnixNano())
}
