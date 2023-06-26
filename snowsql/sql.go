package snowsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"gitlab.com/tymonx/go-formatter/formatter"
	"golang.org/x/exp/slices"
)

func CreateExternalStage(db *sql.DB, stageName, s3WorkspaceURL string, cred credentials.Value) error {
	sql, err := formatter.Format(`
CREATE OR REPLACE STAGE "{stageName}"
URL = '{url}'
CREDENTIALS = (AWS_KEY_ID = '{awsKeyId}' AWS_SECRET_KEY = '{awsSecretKey}' AWS_TOKEN = '{awsToken}')
FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"');
	`, formatter.Named{
		"stageName":    stageName,            // FIXME: Quote
		"url":          s3WorkspaceURL,       // FIXME: Quote
		"awsKeyId":     cred.AccessKeyID,     // FIXME: Quote
		"awsSecretKey": cred.SecretAccessKey, // FIXME: Quote
		"awsToken":     cred.SessionToken,    // FIXME: Quote
	})
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(sql)
	return err
}

func GenCreateExternalStage(stageName, s3WorkspaceURL string, storageIntegration string) string {
	// TO BE DEPRECATED
	return fmt.Sprintf(`
CREATE OR REPLACE STAGE "%s"
STORAGE_INTEGRATION = "%s"
URL = '%s'
FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"');
	`, stageName, storageIntegration, s3WorkspaceURL)
}

func GenCreateInternalStage(stageName string) string {
	return fmt.Sprintf(`
CREATE OR REPLACE STAGE "%s"
FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"');
	`, stageName)
}

func GenDropStage(stageName string) string {
	return fmt.Sprintf(`
DROP STAGE IF EXISTS "%s";
	`, stageName)
}

func GenLoadSnapshotFromStage(targetTable, stageName, fileName string) string {
	// TODO: Load more data?
	return fmt.Sprintf(`
COPY INTO "%s"
FROM '@"%s"%s'
ON_ERROR = CONTINUE;
	`, targetTable, stageName, fileName)
}

func GenCreateSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) (string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", sourceDatabase, sourceTable) // FIXME: Escape
	rows, err := sourceTiDBConn.QueryContext(context.Background(), query)
	if err != nil {
		return "", err
	}
	// TODO: Confirm with generated column, sequence.
	results, err := export.GetSpecifiedColumnValuesAndClose(rows, "FIELD", "TYPE")
	if err != nil {
		return "", err
	}

	snowflakeFieldNames := make([]string, 0)
	snowflakeFieldTypes := make([]string, 0)

	// Ref: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
	for _, oneRow := range results {
		fieldName, tp := oneRow[0], oneRow[1]
		snowflakeFieldNames = append(snowflakeFieldNames, fmt.Sprintf(`"%s"`, fieldName)) // FIXME: Escape
		tpParts := strings.SplitN(tp, "(", 2)
		tpBase := tpParts[0]
		switch tpBase {
		case "bit":
			// TODO
			return "", fmt.Errorf("unsupported field type %s", tp)
		case "text":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TEXT")
		case "date":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "DATE")
		case "datetime":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "DATETIME")
		case "unspecified":
			return "", fmt.Errorf("unsupported field type %s", tp)
		case "decimal":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "DECIMAL")
		case "double":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "DOUBLE")
		case "enum":
			return "", fmt.Errorf("unsupported field type %s", tp)
		case "float":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "FLOAT")
		case "geometry":
			return "", fmt.Errorf("unsupported field type %s", tp)
		case "mediumint":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "INT")
		case "json":
			return "", fmt.Errorf("unsupported field type %s", tp)
		case "int":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "INT")
		case "bigint":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "BIGINT")
		case "longtext":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TEXT")
		case "longblob":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TEXT")
		case "mediumtext":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TEXT")
		case "mediumblob":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TEXT")
		case "null":
			return "", fmt.Errorf("unsupported field type %s", tp)
		case "set":
			return "", fmt.Errorf("unsupported field type %s", tp)
		case "smallint":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "SMALLINT")
		case "char":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "VARCHAR")
		case "binary":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "VARBINARY")
		case "time":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TIME")
		case "timestamp":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TIMESTAMP")
		case "tinyint":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TINYINT")
		case "tinytext":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TEXT")
		case "tinyblob":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "TEXT")
		case "varchar":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "VARCHAR")
		case "varbinary":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "VARBINARY")
		case "var_string":
			snowflakeFieldTypes = append(snowflakeFieldTypes, "VARCHAR")
		case "year":
			return "", fmt.Errorf("unsupported field type %s", tp)
		default:
			return "", fmt.Errorf("unknown field type %s", tp)
		}
	}

	indexQuery := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", sourceDatabase, sourceTable) // FIXME: Escape
	indexRows, err := sourceTiDBConn.QueryContext(context.Background(), indexQuery)
	if err != nil {
		return "", err
	}
	indexResults, err := export.GetSpecifiedColumnValuesAndClose(indexRows, "KEY_NAME", "COLUMN_NAME", "SEQ_IN_INDEX")
	if err != nil {
		return "", err
	}

	snowflakePKColumns := make([]string, 0)
	// Sort by key_name, seq_in_index
	slices.SortFunc(indexResults, func(i, j []string) bool {
		if i[0] == j[0] {
			return i[2] < j[2] // Sort by seq_in_index
		}
		return i[0] < j[0] // Sort by key_name
	})
	for _, oneRow := range indexResults {
		keyName, columnName := oneRow[0], oneRow[1]
		if keyName == "PRIMARY" {
			snowflakePKColumns = append(snowflakePKColumns, fmt.Sprintf(`"%s"`, columnName)) // FIXME: Escape
		}
	}

	// TODO: Support unique key

	sqlRows := make([]string, 0)
	for i := 0; i < len(snowflakeFieldNames); i++ {
		thisField := fmt.Sprintf("%s %s", snowflakeFieldNames[i], snowflakeFieldTypes[i])
		sqlRows = append(sqlRows, thisField)
	}
	if len(snowflakePKColumns) > 0 {
		sqlRows = append(sqlRows, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(snowflakePKColumns, ", ")))
	}
	// Add idents
	for i := 0; i < len(sqlRows); i++ {
		sqlRows[i] = fmt.Sprintf("    %s", sqlRows[i])
	}

	sql := []string{}
	sql = append(sql, fmt.Sprintf(`CREATE OR REPLACE TABLE "%s" (`, sourceTable)) // TODO: Escape
	sql = append(sql, strings.Join(sqlRows, ",\n"))
	sql = append(sql, ")")

	return strings.Join(sql, "\n"), nil
}

func GenMergeInto(tableDef cloudstorage.TableDefinition, filePath string, stageName string) string {
	selectStat := make([]string, 0, len(tableDef.Columns)+1)
	selectStat = append(selectStat, `$1 AS "METADATA$FLAG"`)
	for i, col := range tableDef.Columns {
		selectStat = append(selectStat, fmt.Sprintf(`$%d AS "%s"`, i+5, col.Name))
	}

	pkColumn := make([]string, 0)
	onStat := make([]string, 0)
	for _, col := range tableDef.Columns {
		if col.IsPK == "true" {
			pkColumn = append(pkColumn, fmt.Sprintf(`"%s"`, col.Name))
			onStat = append(onStat, fmt.Sprintf(`T."%s" = S."%s"`, col.Name, col.Name))
		}
	}

	updateStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		updateStat = append(updateStat, fmt.Sprintf(`"%s" = S."%s"`, col.Name, col.Name))
	}

	insertStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		insertStat = append(insertStat, fmt.Sprintf(`"%s"`, col.Name))
	}

	valuesStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		valuesStat = append(valuesStat, fmt.Sprintf(`S."%s"`, col.Name))
	}

	// TODO: Remove QUALIFY row_number() after cdc support merge dml or snowflake support deterministic merge
	mergeQuery := fmt.Sprintf(
		`MERGE INTO "%s" AS T USING
		(
			SELECT
				%s
			FROM '@"%s"/%s'
			QUALIFY row_number() over (partition by %s order by $4 desc) = 1
		) AS S
		ON
		(
			%s
		)
		WHEN MATCHED AND S.METADATA$FLAG = 'U' THEN UPDATE SET %s
		WHEN MATCHED AND S.METADATA$FLAG = 'D' THEN DELETE
		WHEN NOT MATCHED AND S.METADATA$FLAG != 'D' THEN INSERT (%s) VALUES (%s);`,
		tableDef.Table,
		strings.Join(selectStat, ",\n"),
		stageName,
		filePath,
		strings.Join(pkColumn, ", "),
		strings.Join(onStat, " AND "),
		strings.Join(updateStat, ", "),
		strings.Join(insertStat, ", "),
		strings.Join(valuesStat, ", "))

	return mergeQuery
}
