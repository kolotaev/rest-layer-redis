package rds_test

import (
	"testing"
	"context"

	"github.com/go-redis/redis"
	"github.com/rs/rest-layer/schema"
	"github.com/stretchr/testify/suite"

	rds "github.com/kolotaev/rest-layer-redis"
)

const redisAddress = "127.0.0.1:6379"
const usersEntity = "users"

var userSchema = schema.Schema{
	Fields: schema.Fields{
		"id":      schema.IDField,
		"updated": schema.UpdatedField,
		"name": {
			Required:   true,
			Filterable: true,
			Sortable:   true,
			Validator: &schema.String{
				MaxLen: 150,
			},
		},
		"age": {
			Required:   true,
			Filterable: true,
			Sortable:   true,
			Validator: &schema.Integer{},
		},
		"birth": {
			Required:   true,
			Filterable: true,
			Sortable:   false,
			Validator: &schema.Time{},
		},
		"height": {
			Required:   true,
			Filterable: true,
			Sortable:   false,
			Validator: &schema.Float{},
		},
		"male": {
			Required:   true,
			Filterable: true,
			Sortable:   true,
			Default: false,
			Validator: &schema.Bool{},
		},
	},
}

type RedisMainTestSuite struct {
	suite.Suite

	client *redis.Client
	handler *rds.Handler
	ctx context.Context
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInsertSuite(t *testing.T) {
	suite.Run(t, new(RedisMainTestSuite))
}

func (s *RedisMainTestSuite) SetupSuite() {
	s.client = redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})

	_, err := s.client.Ping().Result()
	if err != nil {
		s.T().Fatal(err)
	}

	s.handler = rds.NewHandler(s.client, usersEntity, userSchema)
	s.ctx = context.Background()
}

// Make sure that Redis DB is clean before each test
// before each test
func (s *RedisMainTestSuite) SetupTest() {
	s.client.FlushAll()
}

// Make sure that Redis DB is clean after each test
// before each test
func (s *RedisMainTestSuite) TeardownTest() {
	s.client.FlushAll()
}
