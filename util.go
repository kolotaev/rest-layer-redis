package rds

import (
	"bytes"
	"runtime"
	"strconv"
	"sort"
	"fmt"
	"strings"
)

// Obtain go-routine ID.
// It's not the best practice to use goroutine's id but in our case it's justifiable.
// ToDo: is it performant and reliable?
func getGoRoutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

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

// Creates a lua table.
func makeLuaTable(a []interface{}) string {
	return fmt.Sprintf("{" + strings.Repeat("'%s',", len(a)) + "}", a)
}
