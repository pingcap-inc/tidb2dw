package pin14e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap-inc/tidb2dw/pkg/testfixtures"
	"github.com/snowflakedb/gosnowflake"
)

type realS3Client struct {
	svc *s3.S3
}

type realSnowflakeClient struct {
	db                  *sql.DB
	fixtureDir          string
	stageCredentialMode stageCredentialMode
}

func NewRealClients(cfg Config) (StorageClient, SnowflakeClient, error) {
	cfg = normalizeConfig(cfg)
	if err := ValidateConfig(cfg); err != nil {
		return nil, nil, err
	}
	awsConfig := &aws.Config{Region: aws.String(cfg.AWSRegion)}
	if cfg.AWSAccessKeyID != "" || cfg.AWSSecretAccessKey != "" || cfg.AWSSessionToken != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSSessionToken)
	}
	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("create AWS session: %w", err)
	}
	sfConfig := &gosnowflake.Config{
		Account:   cfg.SnowflakeAccountID,
		User:      cfg.SnowflakeUser,
		Password:  cfg.SnowflakePassword,
		Warehouse: cfg.SnowflakeWarehouse,
	}
	dsn, err := gosnowflake.DSN(sfConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("create Snowflake DSN: %w", err)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open Snowflake connection: %w", err)
	}
	if err = db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("ping Snowflake: %w", err)
	}
	return &realS3Client{svc: s3.New(awsSession)}, &realSnowflakeClient{
		db:                  db,
		fixtureDir:          cfg.FixtureDir,
		stageCredentialMode: newStageCredentialMode(cfg),
	}, nil
}

func (c *realS3Client) PutObject(ctx context.Context, localPath, objectURI string) error {
	bucket, key, err := parseS3URI(objectURI)
	if err != nil {
		return err
	}
	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = c.svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	return err
}

func (c *realS3Client) DeletePrefix(ctx context.Context, prefixURI string) (int, error) {
	bucket, prefix, err := parseS3URI(prefixURI)
	if err != nil {
		return 0, err
	}
	var deleted int
	err = c.svc.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(strings.TrimPrefix(prefix, "/")),
	}, func(page *s3.ListObjectsV2Output, _ bool) bool {
		if len(page.Contents) == 0 {
			return true
		}
		objects := make([]*s3.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			objects = append(objects, &s3.ObjectIdentifier{Key: object.Key})
		}
		_, deleteErr := c.svc.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if deleteErr != nil {
			err = deleteErr
			return false
		}
		deleted += len(objects)
		return true
	})
	return deleted, err
}

func (c *realSnowflakeClient) PrepareNamespace(ctx context.Context, database, schema string) error {
	if _, err := c.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(database)); err != nil {
		return err
	}
	if _, err := c.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdent(database)+"."+quoteIdent(schema)); err != nil {
		return err
	}
	_, err := c.db.ExecContext(ctx, "USE SCHEMA "+quoteIdent(database)+"."+quoteIdent(schema))
	return err
}

func (c *realSnowflakeClient) LoadSnapshot(ctx context.Context, scenario ScenarioRun) error {
	initialCols, finalCols, err := c.scenarioColumns(scenario)
	if err != nil {
		return err
	}
	if err := c.createScenarioStages(ctx, scenario); err != nil {
		return err
	}
	if _, err := c.db.ExecContext(ctx, "CREATE OR REPLACE TABLE "+quoteIdent(scenario.Table)+" ("+columnDefs(finalCols)+")"); err != nil {
		return err
	}
	temp := tempName(scenario, "SNAPSHOT")
	if _, err := c.db.ExecContext(ctx, "CREATE OR REPLACE TEMP TABLE "+quoteIdent(temp)+" ("+columnDefs(initialCols)+")"); err != nil {
		return err
	}
	for _, rel := range scenario.Manifest.SnapshotFiles {
		if !strings.HasSuffix(rel, ".csv") {
			continue
		}
		if err := c.copyInto(ctx, temp, stageName(scenario, "SNAPSHOT"), rel); err != nil {
			return err
		}
	}
	common := commonColumns(initialCols, finalCols)
	if len(common) == 0 {
		return fmt.Errorf("scenario %s has no common snapshot/final columns", scenario.Manifest.ScenarioID)
	}
	_, err = c.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s",
		quoteIdent(scenario.Table),
		quoteIdentList(common),
		quoteIdentList(common),
		quoteIdent(temp),
	))
	return err
}

func (c *realSnowflakeClient) ApplyCDC(ctx context.Context, scenario ScenarioRun) error {
	_, finalCols, err := c.scenarioColumns(scenario)
	if err != nil {
		return err
	}
	if timelineContains(scenario, "TRUNCATE TABLE") || timelineContains(scenario, "DROP TABLE") {
		if _, err := c.db.ExecContext(ctx, "TRUNCATE TABLE "+quoteIdent(scenario.Table)); err != nil {
			return err
		}
	}
	temp := tempName(scenario, "CDC")
	metadataCols := []string{"METADATA_FLAG", "METADATA_SCHEMA", "METADATA_TABLE", "METADATA_COMMIT_TS"}
	allCols := append(append([]string{}, metadataCols...), finalCols...)
	if _, err := c.db.ExecContext(ctx, "CREATE OR REPLACE TEMP TABLE "+quoteIdent(temp)+" ("+columnDefs(allCols)+")"); err != nil {
		return err
	}
	for _, rel := range scenario.Manifest.IncrementFiles {
		if !strings.HasSuffix(rel, ".csv") {
			continue
		}
		if err := c.copyInto(ctx, temp, stageName(scenario, "INCREMENT"), rel); err != nil {
			return err
		}
	}
	if len(finalCols) == 0 {
		return fmt.Errorf("scenario %s has no final columns", scenario.Manifest.ScenarioID)
	}
	assignments := make([]string, 0, len(finalCols))
	for _, col := range finalCols {
		assignments = append(assignments, fmt.Sprintf("T.%s = S.%s", quoteIdent(col), quoteIdent(col)))
	}
	mergeSQL := fmt.Sprintf(`MERGE INTO %s AS T USING (
SELECT %s FROM %s
QUALIFY row_number() over (partition by %s order by %s desc) = 1
) AS S ON T.%s = S.%s
WHEN MATCHED AND S.%s = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET %s
WHEN NOT MATCHED AND S.%s != 'D' THEN INSERT (%s) VALUES (%s)`,
		quoteIdent(scenario.Table),
		quoteIdentList(allCols),
		quoteIdent(temp),
		quoteIdent("id"),
		quoteIdent("METADATA_COMMIT_TS"),
		quoteIdent("id"),
		quoteIdent("id"),
		quoteIdent("METADATA_FLAG"),
		strings.Join(assignments, ", "),
		quoteIdent("METADATA_FLAG"),
		quoteIdentList(finalCols),
		prefixedIdentList("S", finalCols),
	)
	_, err = c.db.ExecContext(ctx, mergeSQL)
	return err
}

func (c *realSnowflakeClient) FetchValidation(ctx context.Context, scenario ScenarioRun) (ActualOutcome, error) {
	_, finalCols, err := c.scenarioColumns(scenario)
	if err != nil {
		return ActualOutcome{}, err
	}
	rows, err := c.db.QueryContext(ctx, "SELECT "+quoteIdentList(finalCols)+" FROM "+quoteIdent(scenario.Table)+" ORDER BY "+quoteIdent("id"))
	if err != nil {
		return ActualOutcome{}, err
	}
	defer rows.Close()
	actualRows := make([]testfixtures.Row, 0)
	selected := map[string]testfixtures.Row{}
	values := make([]sql.NullString, len(finalCols))
	scans := make([]interface{}, len(finalCols))
	for i := range values {
		scans[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return ActualOutcome{}, err
		}
		row := testfixtures.Row{}
		for i, col := range finalCols {
			if values[i].Valid {
				row[col] = values[i].String
			} else {
				row[col] = `\N`
			}
		}
		actualRows = append(actualRows, row)
		if id := row["id"]; id == "2" || id == "3" || id == "100001" {
			selected[id] = row
		}
	}
	if err := rows.Err(); err != nil {
		return ActualOutcome{}, err
	}
	return ActualOutcome{
		SchemaColumns: finalCols,
		RowCount:      len(actualRows),
		Checksum:      checksumRows(actualRows, finalCols),
		SelectedRows:  selected,
	}, nil
}

func (c *realSnowflakeClient) Cleanup(ctx context.Context, owned OwnedSnowflakeObjects) (CleanupSnowflakeSummary, error) {
	var summary CleanupSnowflakeSummary
	var firstErr error
	if owned.Database != "" && owned.Schema != "" {
		if _, err := c.db.ExecContext(ctx, "USE SCHEMA "+quoteIdent(owned.Database)+"."+quoteIdent(owned.Schema)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, stage := range uniqueStrings(owned.Stages) {
		if _, err := c.db.ExecContext(ctx, "DROP STAGE IF EXISTS "+quoteIdent(stage)); err != nil && firstErr == nil {
			firstErr = err
		} else if err == nil {
			summary.StagesDropped++
		}
	}
	for _, table := range uniqueStrings(owned.Tables) {
		if _, err := c.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(table)); err != nil && firstErr == nil {
			firstErr = err
		} else if err == nil {
			summary.TablesDropped++
		}
	}
	return summary, firstErr
}

func (c *realSnowflakeClient) createScenarioStages(ctx context.Context, scenario ScenarioRun) error {
	for _, kind := range []string{"SNAPSHOT", "INCREMENT"} {
		stage := stageName(scenario, kind)
		sqlText := createStageSQL(stage, scenario.StorageRootURI, c.stageCredentialMode)
		if _, err := c.db.ExecContext(ctx, sqlText); err != nil {
			return err
		}
	}
	return nil
}

type stageCredentialMode struct {
	storageIntegration string
	awsAccessKeyID     string
	awsSecretAccessKey string
	awsSessionToken    string
}

func newStageCredentialMode(cfg Config) stageCredentialMode {
	if cfg.SnowflakeStorageIntegration != "" {
		return stageCredentialMode{storageIntegration: cfg.SnowflakeStorageIntegration}
	}
	return stageCredentialMode{
		awsAccessKeyID:     cfg.AWSAccessKeyID,
		awsSecretAccessKey: cfg.AWSSecretAccessKey,
		awsSessionToken:    cfg.AWSSessionToken,
	}
}

func createStageSQL(stage, storageRootURI string, mode stageCredentialMode) string {
	credentialSQL := "STORAGE_INTEGRATION = " + quoteIdent(mode.storageIntegration)
	if mode.storageIntegration == "" {
		parts := []string{
			"AWS_KEY_ID='" + escapeSQLString(mode.awsAccessKeyID) + "'",
			"AWS_SECRET_KEY='" + escapeSQLString(mode.awsSecretAccessKey) + "'",
		}
		if mode.awsSessionToken != "" {
			parts = append(parts, "AWS_TOKEN='"+escapeSQLString(mode.awsSessionToken)+"'")
		}
		credentialSQL = "CREDENTIALS = (" + strings.Join(parts, " ") + ")"
	}
	return fmt.Sprintf(
		"CREATE OR REPLACE STAGE %s URL = '%s' %s FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='\"' ESCAPE='\\\\' NULL_IF=())",
		quoteIdent(stage),
		escapeSQLString(storageRootURI),
		credentialSQL,
	)
}

func (c *realSnowflakeClient) copyInto(ctx context.Context, table, stage, relPath string) error {
	copySQL := fmt.Sprintf(
		"COPY INTO %s FROM @%s/%s FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='\"' ESCAPE='\\\\' NULL_IF=())",
		quoteIdent(table),
		quoteIdent(stage),
		escapeStagePath(relPath),
	)
	_, err := c.db.ExecContext(ctx, copySQL)
	return err
}

func (c *realSnowflakeClient) scenarioColumns(scenario ScenarioRun) ([]string, []string, error) {
	var initial []string
	var latest []string
	for _, rel := range scenario.Manifest.IncrementFiles {
		if !strings.HasSuffix(rel, ".json") {
			continue
		}
		def, err := readTableDefinition(filepath.Join(c.fixtureDir, filepath.FromSlash(rel)))
		if err != nil {
			return nil, nil, err
		}
		cols := make([]string, 0, len(def.Columns))
		for _, col := range def.Columns {
			cols = append(cols, col.Name)
		}
		if initial == nil {
			initial = cols
		}
		latest = cols
	}
	if initial == nil {
		return nil, nil, fmt.Errorf("scenario %s has no schema definition", scenario.Manifest.ScenarioID)
	}
	if len(scenario.Manifest.Expected.FinalSchemaColumns) > 0 {
		latest = scenario.Manifest.Expected.FinalSchemaColumns
	}
	return initial, latest, nil
}

func readTableDefinition(path string) (testfixtures.TableDefinition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return testfixtures.TableDefinition{}, err
	}
	var def testfixtures.TableDefinition
	if err := json.Unmarshal(data, &def); err != nil {
		return testfixtures.TableDefinition{}, err
	}
	return def, nil
}

func parseS3URI(raw string) (string, string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", err
	}
	if parsed.Scheme != "s3" || parsed.Host == "" {
		return "", "", fmt.Errorf("expected s3:// URI")
	}
	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteIdentList(cols []string) string {
	out := make([]string, len(cols))
	for i, col := range cols {
		out[i] = quoteIdent(col)
	}
	return strings.Join(out, ", ")
}

func prefixedIdentList(prefix string, cols []string) string {
	out := make([]string, len(cols))
	for i, col := range cols {
		out[i] = prefix + "." + quoteIdent(col)
	}
	return strings.Join(out, ", ")
}

func columnDefs(cols []string) string {
	defs := make([]string, len(cols))
	for i, col := range cols {
		defs[i] = quoteIdent(col) + " VARCHAR"
	}
	return strings.Join(defs, ", ")
}

func commonColumns(a, b []string) []string {
	seen := map[string]struct{}{}
	for _, col := range a {
		seen[col] = struct{}{}
	}
	var common []string
	for _, col := range b {
		if _, ok := seen[col]; ok {
			common = append(common, col)
		}
	}
	return common
}

func timelineContains(scenario ScenarioRun, needle string) bool {
	needle = strings.ToUpper(needle)
	for _, event := range scenario.Manifest.Timeline {
		if strings.Contains(strings.ToUpper(event.SQL), needle) {
			return true
		}
	}
	return false
}

func tempName(scenario ScenarioRun, suffix string) string {
	return cleanSnowflakeName(fmt.Sprintf("%s_%s_%s_%s_TMP", scenario.Manifest.Tenant.TenantID, scenario.Manifest.Tenant.TaskID, scenario.Manifest.ScenarioID, suffix))
}

func cleanSnowflakeName(value string) string {
	value = strings.ToUpper(value)
	re := regexp.MustCompile(`[^A-Z0-9_]+`)
	value = re.ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return "PIN14_OBJECT"
	}
	return value
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func escapeStagePath(value string) string {
	return strings.TrimPrefix(path.Clean("/"+value), "/")
}

func checksumRows(rows []testfixtures.Row, columns []string) string {
	hash := fnv.New64a()
	sort.Slice(rows, func(i, j int) bool { return rows[i]["id"] < rows[j]["id"] })
	for _, row := range rows {
		for _, column := range columns {
			io.WriteString(hash, column)
			io.WriteString(hash, "=")
			io.WriteString(hash, row[column])
			io.WriteString(hash, ";")
		}
	}
	return fmt.Sprintf("fnv64a:%016x", hash.Sum64())
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
