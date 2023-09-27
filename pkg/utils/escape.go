package utils

import (
	"strconv"
	"strings"
)

func EscapeString(s string) string {
	// See https://docs.snowflake.com/en/sql-reference/data-types-text#escape-sequences-in-single-quoted-string-constants
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		r := s[i]
		if strconv.IsPrint(rune(r)) {
			sb.WriteByte(r)
			continue
		}
		switch r {
		case '\'':
			sb.Write([]byte{'\\', '\''})
		case '"':
			sb.Write([]byte{'\\', '"'})
		case '\\':
			sb.Write([]byte{'\\', '\\'})
		case '\b':
			sb.Write([]byte{'\\', 'b'})
		case '\f':
			sb.Write([]byte{'\\', 'f'})
		case '\n':
			sb.Write([]byte{'\\', 'n'})
		case '\r':
			sb.Write([]byte{'\\', 'r'})
		case '\t':
			sb.Write([]byte{'\\', 't'})
		case 0:
			sb.Write([]byte{'\\', '0'})
		default:
			sb.WriteString("\\u")
			sb.WriteString(strconv.FormatInt(int64(r), 16))
		}
	}
	return sb.String()
}
