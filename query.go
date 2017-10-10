package rds

// getField translates a schema field into a Redis field:
func getField(f string) string {
	if f == "id" {
		return "__id__"
	}
	return f
}
