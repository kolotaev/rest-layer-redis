package rds_test

import (
	"testing"

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
		"created": schema.CreatedField,
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

type InsertTestSuite struct {
	suite.Suite

	client *redis.Client
	handler *rds.Handler
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInsertTestSuite(t *testing.T) {
	suite.Run(t, new(InsertTestSuite))
}

func (s *InsertTestSuite) SetupSuite() {
	s.client = redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})

	_, err := s.client.Ping().Result()
	if err != nil {
		s.T().Fatal(err)
	}

	s.handler = rds.NewHandler(s.client, usersEntity, userSchema)
}

// Make sure that Redis DB is clean before each test
// before each test
func (s *InsertTestSuite) SetupTest() {
	s.client.FlushAll()
}


func (s *InsertTestSuite) TestInsert() {
	s.NotEqual(90, 89)
}
