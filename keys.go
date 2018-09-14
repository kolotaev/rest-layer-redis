package rds

import "fmt"

const (
	auxIndexListSortedSuffix = ":secondary_idx_zset_list"
	auxIndexListNonSortedSuffix = ":secondary_idx_set_list"
	// TODO - can we use something already existing?
	allIDsSuffix = "all_ids"
)

// Get key name for a Redis set.
// Ex: users:hair-color:brown
func sKey(entity, key string, value interface{}) string {
	return fmt.Sprintf("%s:%s:%v", entity, key, value)
}

// Get key name for a Redis sorted set.
// Ex: users:age
func zKey(entity, key string) string {
	return fmt.Sprintf("%s:%s", entity, key)
}

// Get a search pattern for the last element of a compound key (for Redis set).
// Ex: users:hair-color:* -> get all stored ages of users.
func sKeyLastAll(entity, key string) string {
	return fmt.Sprintf("%s:%s:*", entity, key)
}

// Get an IDs key name for set of all entity IDs.
// Ex: users:ids
func sKeyIDsAll(entity string) string {
	return fmt.Sprintf("%s:%s", entity, allIDsSuffix)
}