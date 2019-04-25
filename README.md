# ⚠️ WIP !!!
# REST Layer Redis Backend

[![Build Status](https://travis-ci.org/kolotaev/rest-layer-redis.svg?branch=master)](https://travis-ci.org/kolotaev/rest-layer-redis)
[![codecov.io](https://codecov.io/github/kolotaev/rest-layer-redis/coverage.svg?branch=master)](https://codecov.io/github/kolotaev/rest-layer-redis?branch=master)

## Usage

Storage uses Lua scripting, available only since Redis version 2.6 or greater.

```go
import (
    "github.com/go-redis/redis"
    "github.com/kolotaev/rest-layer-redis"
)
```

Create a redis client:

```go
client := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    // Any other viable config values here
})

// Check availability of the Redis server
pong, err := client.Ping().Result()
```

Create entities of your domain and a resource storage handlers for them:

```go
user := schema.Schema{
		Description: `The User model`,
		Fields: schema.Fields{
			"name": {
				Required: true,
				ReadOnly: true,
				OnInit: schema.NewID,
				Filterable: true,
				Sortable:   true,
			},
			"age": {
				Required:   true,
				Filterable: true,
			},
		},
	}
usersHandler := redis.NewHandler(client, "user", user)

posts := schema.Schema{/* ... */}
postsHandler := rds.NewHandler(client, "posts", posts)
```

Use this handler with a resource:

```go
index.Bind("users", user, usersHandler, resource.DefaultConf)
index.Bind("posts", posts, postsHandler, resource.DefaultConf)
```

You may want to create many Redis handlers as you have resources as long as you want each resources in a
different collection. You can share the same `Redis` session across all you handlers.


## Things you should be aware of

- Under the hood storage handler creates secondary indices inside Redis for proper filtering support. These indices are
created/updated/deleted for every `Filterable` field on every entity record. You should no worry about it, but don't
be confused if you see some unknown sets in Redis explorer.

- Storage handler heavily relies on types of resource fields to process results retrieved from Redis.
So it's better you specify `Validator` type for every field - otherwise results coerced to string.

- Sorting by more than 1 field is not supported due to Redis query semantics restriction.


## License

Copyright © 2017-2019 Egor Kolotaev.

Distributed under MIT License.
