package rds

//import (
//	"github.com/rs/rest-layer/schema/query"
//	"github.com/rs/rest-layer/resource"
//	"gopkg.in/mgo.v2/bson"
//)
//
//// getField translates a schema field into a Redis field:
//func getField(f string) string {
//	if f == "id" {
//		return "__id__"
//	}
//	return f
//}
//
//func translatePredicate(q query.Predicate) (string, error) {
//	ps := make([]string, 0)
//	for _, exp := range q {
//		switch t := exp.(type) {
//		case query.And:
//			s := []bson.M{}
//			for _, subExp := range t {
//				sb, err := translatePredicate(query.Predicate{subExp})
//				if err != nil {
//					return nil, err
//				}
//				s = append(s, sb)
//			}
//			b["$and"] = s
//		case query.Or:
//			s := []bson.M{}
//			for _, subExp := range t {
//				sb, err := translatePredicate(query.Predicate{subExp})
//				if err != nil {
//					return nil, err
//				}
//				s = append(s, sb)
//			}
//			b["$or"] = s
//		case query.In:
//			b[getField(t.Field)] = bson.M{"$in": valuesToInterface(t.Values)}
//		case query.NotIn:
//			b[getField(t.Field)] = bson.M{"$nin": valuesToInterface(t.Values)}
//		case query.Equal:
//			b[getField(t.Field)] = t.Value
//		case query.NotEqual:
//			b[getField(t.Field)] = bson.M{"$ne": t.Value}
//		case query.GreaterThan:
//			b[getField(t.Field)] = bson.M{"$gt": t.Value}
//		case query.GreaterOrEqual:
//			b[getField(t.Field)] = bson.M{"$gte": t.Value}
//		case query.LowerThan:
//			b[getField(t.Field)] = bson.M{"$lt": t.Value}
//		case query.LowerOrEqual:
//			b[getField(t.Field)] = bson.M{"$lte": t.Value}
//		case query.Regex:
//			b[getField(t.Field)] = bson.M{"$regex": t.Value.String()}
//		default:
//			return nil, resource.ErrNotImplemented
//		}
//	}
//	return b, nil
//}
//
//func getQuery(q *query.Query) (string, error) {
//	return translatePredicate(q.Predicate)
//}
