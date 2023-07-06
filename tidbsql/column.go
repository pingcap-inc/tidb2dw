package tidbsql

type TiDBColumnInfo struct {
	ColumnName    string
	ColumnDefault *string
	IsNullable    string
	DataType      string
	CharMaxLength *int
	NumPrecision  *int
	NumScale      *int
	DateTimePrec  *int
}
