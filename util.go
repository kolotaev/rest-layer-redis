package rds

import (
	"context"
	"fmt"
	"time"
	"math"

	"github.com/rs/rest-layer/schema/query"
)

// Determine if value is numeric.
// Numeric values are all ints, floats, time values.
func isNumeric(v ...query.Value) bool {
	switch v[0].(type) {
	case int, int8, int16, int32, int64, float32, float64, time.Time:
		return true
	default:
		return false
	}
}

// TODO - better?
// TODO See toFloat64
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

func toFloat64(in query.Value) float64 {
	switch v := in.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	}
	return math.NaN()
}

// TODO -1 ?
func toInt(in query.Value) int {
	switch v := in.(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	}
	return -1
}

func inSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// handleWithContext makes requests to Redis aware of context.
// Additionally it checks if we already have context error before proceeding further.
// Rationale: redis-go actually doesn't support context abortion on its operations, though it has WithContext() client.
// See: https://github.com/go-redis/redis/issues/582
func handleWithContext(ctx context.Context, handler func() error) error {
	var err error

	if err = ctx.Err(); err != nil {
		return err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		err = handler()
	}()

	select {
	case <-ctx.Done():
		// Monitor context cancellation. Cancellation may happen if the client closed the connection
		// or if the configured request timeout has been reached.
		return ctx.Err()
	case <-done:
		// Wait until Redis command finishes.
		return err
	}
}

func pr(v ...interface{}) {
	for _, i := range v {
		fmt.Printf("%#v\n", i)
	}
}
