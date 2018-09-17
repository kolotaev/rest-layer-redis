package rds

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"
	"errors"

	"github.com/rs/rest-layer/schema/query"
)

// getRangeNumericPairs creates consequent combinations of ASC-sorted input elements.
// Values are supposed to be numeric.
// And all of them are cast to either int or float64 to make heterogeneous elements sorting.
// If input contains both ints and floats - error is returned.
// If input contains non-numeric values - error is returned.
// Is used for creating range tuples for Lua.
// Ex: [4, 77, 15, 9, 0] -> ["{'-inf',0}", "{0,4}", "{4,9}", ... "{77,'+inf'}"]
func getRangeNumericPairs(in []query.Value) ([]string, error) {
	toSortInts := make([]int, 0, len(in))
	toSortFloats := make([]float64, 0, len(in))
	stringedNums := []string{"'-inf'"}
	allInts, allFloats := true, true

	if len(in) == 0 {
		return []string{"{'-inf','+inf'}"}, nil
	}

	for _, i := range in {
		switch v := i.(type) {
		case float32, float64:
			allInts = false
			toSortFloats = append(toSortFloats, toFloat64(v))
		case int, int8, int16, int32, int64:
			allFloats = false
			toSortInts = append(toSortInts, toInt(v))
		default:
			allInts = false
			allFloats = false
		}
	}

	if allInts {
		sort.Ints(toSortInts)
		for _, i := range toSortInts {
			stringedNums = append(stringedNums, fmt.Sprintf("%d", i))
		}
	} else if allFloats {
		sort.Float64s(toSortFloats)
		for _, i := range toSortFloats {
			stringedNums = append(stringedNums, fmt.Sprintf("%.6f", i))
		}
	} else {
		return []string{}, errors.New("Input data has mixed values type. Accepted only integers or only floats")
	}

	var out []string
	stringedNums = append(stringedNums, "'+inf'")
	for i := 1; i < len(stringedNums); i++ {
		var tuple = fmt.Sprintf("{%s,%s}", stringedNums[i - 1], stringedNums[i])
		out = append(out, tuple)
	}
	return out, nil
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
	aQuoted := make([]string, 0, len(a))
	for _, v := range a {
		aQuoted = append(aQuoted, quoteValue(v))
	}
	return fmt.Sprintf("{%s}", strings.Join(aQuoted, ","))
}

// Generate random string suited for temporary Lua variable and Redis key
func tmpVar() string {
	return fmt.Sprintf("tmp_%d_%d", rand.Int(), time.Now().UnixNano())
}

// quoteValue returns quoted or unquoted value for Lua consumption
func quoteValue(v interface{}) string {
	switch val := v.(type) {
	case string, bool:
		return fmt.Sprintf("'%v'", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}
