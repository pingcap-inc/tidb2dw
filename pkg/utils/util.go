package utils

import (
	"strings"
)

// SplitTableFQN splits a full-qualified table name into database and table name
// e.g. "mydb.mytable" -> "mydb", "mytable"
// Note: this function does not check if the input is a valid table name
func SplitTableFQN(tableFQN string) (string, string) {
	parts := strings.SplitN(tableFQN, ".", 2)
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1]
}
