package utils

import "github.com/pingcap/tiflow/pkg/sink/cloudstorage"

var (
	CDCFlagColumnName       = "tidb2dw_flag"
	CDCTablenameColumnName  = "tidb2dw_tablename"
	CDCSchemanameColumnName = "tidb2dw_schemaname"
	CDCCommitTsColumnName   = "tidb2dw_commit_ts"
)

func GenIncrementTableColumns(columns []cloudstorage.TableCol) []cloudstorage.TableCol {
	return append([]cloudstorage.TableCol{
		{
			Name: CDCFlagColumnName,
			Tp:   "varchar",
		},
		{
			Name: CDCTablenameColumnName,
			Tp:   "varchar",
		},
		{
			Name: CDCSchemanameColumnName,
			Tp:   "varchar",
		},
		{
			Name: CDCCommitTsColumnName,
			Tp:   "bigint",
		},
	}, columns...)
}
