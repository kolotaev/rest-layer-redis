package rds

import (
	"fmt"
	"testing"

	"github.com/rs/rest-layer/schema/query"
	"github.com/stretchr/testify/assert"
)

func TestIsNumeric(t *testing.T) {
	cases := []struct {
		value query.Value
		want  bool
	}{
		{query.Value(6), true},
		{query.Value(-6), true},
		{query.Value(1.89), true},
		{query.Value(-1.89), true},
		{query.Value(6.0), true},
		{query.Value(-6.0), true},
		{query.Value(99999999999999999), true},
		{query.Value("foo"), false},
		{query.Value("1"), false},
		{query.Value("-3"), false},
		{query.Value("7.9"), false},
		{query.Value(map[int]int{90: 900}), false},
		{query.Value([]int{90}), false},
		{query.Value(nil), false},
	}
	for i := range cases {
		tc := cases[i]
		assert.Equal(t, tc.want, isNumeric(tc.value), fmt.Sprintf("Test case #%d", i))
	}
}
