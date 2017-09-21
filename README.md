# WIP !!!
# REST Layer Redis Backend


## Usage

```go
import "github.com/go-redis/redis"
import "github.com/kolotaev/rest-layer-redis"
```

Create a redis client:

```go
client := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "", // no password set
    DB:       0,  // use default DB
})

pong, err := client.Ping().Result()
fmt.Println(pong, err)
```

Create a resource storage handler with a given DB/collection:

```go
user := schema.Schema{
		Description: `The user object`,
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
s := redis.NewHandler(client, "user", user)
```

Use this handler with a resource:

```go
index.Bind("users", user, s, resource.DefaultConf)
```

You may want to create a many Redis handlers as you have resources as long as you want each resources in a
different collection. You can share the same `Redis` session across all you handlers.
