package testfixtures

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateIsDeterministicAndScoped(t *testing.T) {
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "unrelated_tenant_data.txt")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o644))

	opts := Options{
		OutputDir:   dir,
		Seed:        42,
		ScenarioIDs: []string{"DDL-V1-001", "DDL-V1-005", "DDL-V1-023", "DDL-V1-028"},
		CleanOutput: true,
	}
	first, err := Generate(opts)
	require.NoError(t, err)
	firstManifest, err := os.ReadFile(filepath.Join(dir, "manifests", "manifest.json"))
	require.NoError(t, err)
	require.FileExists(t, sentinel)

	second, err := Generate(opts)
	require.NoError(t, err)
	secondManifest, err := os.ReadFile(filepath.Join(dir, "manifests", "manifest.json"))
	require.NoError(t, err)
	require.Equal(t, firstManifest, secondManifest)
	require.Len(t, second.Manifests, len(first.Manifests))
	require.FileExists(t, sentinel)
}

func TestGenerateWritesValidTiCDCSchemaAndDMLFlushes(t *testing.T) {
	dir := t.TempDir()
	result, err := Generate(Options{
		OutputDir:   dir,
		Seed:        7,
		ScenarioIDs: []string{"DDL-V1-005"},
		Tenants: []TenantTask{{
			TenantID:          "tenant_test",
			TaskID:            "task_test",
			SourceSchema:      "src_test",
			DestinationSchema: "DW_TEST",
			StoragePrefix:     "tenant_test/task_test",
			CheckpointPrefix:  "tenant_test/task_test/checkpoints",
		}},
		CleanOutput: true,
	})
	require.NoError(t, err)
	require.Len(t, result.Manifests, 1)

	manifest := result.Manifests[0]
	require.GreaterOrEqual(t, len(manifest.IncrementFiles), 6)
	require.Contains(t, manifest.Expected.FinalSchemaColumns, "coupon_code")
	require.NotEmpty(t, manifest.Expected.Checksum)
	require.FileExists(t, filepath.Join(dir, manifest.SnapshotFiles[1]))

	var schemaPath string
	for _, rel := range manifest.IncrementFiles {
		if strings.Contains(rel, "/meta/schema_") {
			schemaPath = rel
		}
	}
	require.NotEmpty(t, schemaPath)

	data, err := os.ReadFile(filepath.Join(dir, schemaPath))
	require.NoError(t, err)
	var tableDef TableDefinition
	require.NoError(t, json.Unmarshal(data, &tableDef))
	generated, err := tableDef.GenerateSchemaFilePath()
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(schemaPath, generated))

	var dmlPath string
	for _, rel := range manifest.IncrementFiles {
		if strings.HasSuffix(rel, ".csv") {
			dmlPath = rel
			break
		}
	}
	require.NotEmpty(t, dmlPath)
	dml, err := os.ReadFile(filepath.Join(dir, dmlPath))
	require.NoError(t, err)
	require.Contains(t, string(dml), "src_test,orders")
}

func TestGenerateAddColumnChecksumUsesSnowflakeNullForExistingRows(t *testing.T) {
	dir := t.TempDir()
	result, err := Generate(Options{
		OutputDir:   dir,
		Seed:        7,
		ScenarioIDs: []string{"DDL-V1-005"},
		Tenants: []TenantTask{{
			TenantID:          "tenant_test",
			TaskID:            "task_test",
			SourceSchema:      "src_test",
			DestinationSchema: "DW_TEST",
			StoragePrefix:     "tenant_test/task_test",
			CheckpointPrefix:  "tenant_test/task_test/checkpoints",
		}},
		CleanOutput: true,
	})
	require.NoError(t, err)
	require.Len(t, result.Manifests, 1)

	manifest := result.Manifests[0]
	baseColumns := baselineColumns()
	finalColumns := columnsForScenario("DDL-V1-005", baseColumns)
	snapshotRows := makeRows(7, profileConfigs[ProfileSmoke].SnapshotRows, baseColumns, "src_test", "orders", "tenant_test", false)
	cdcRows := makeRows(10007, profileConfigs[ProfileSmoke].CDCInserts, finalColumns, "src_test", "orders", "tenant_test", true)
	expectedRows := expectedRowsForScenario("DDL-V1-005", snapshotRows, cdcRows)
	projectedRows := projectRows(expectedRows, finalColumns)

	require.Equal(t, `\N`, projectedRows[0]["coupon_code"])
	require.Equal(t, checksumRows(projectedRows, finalColumns), manifest.Expected.Checksum)
	require.NotEqual(t, checksumRows(expectedRows, finalColumns), manifest.Expected.Checksum)
}

func TestGenerateDropRecreateResetsRowsAndUsesRecreatedSchema(t *testing.T) {
	dir := t.TempDir()
	result, err := Generate(Options{
		OutputDir:   dir,
		Seed:        140,
		Profile:     ProfileE2E,
		ScenarioIDs: []string{"DDL-V1-027"},
		Tenants: []TenantTask{{
			TenantID:          "tenant_test",
			TaskID:            "task_test",
			SourceSchema:      "src_test",
			DestinationSchema: "DW_TEST",
			StoragePrefix:     "tenant_test/task_test",
			CheckpointPrefix:  "tenant_test/task_test/checkpoints",
		}},
		CleanOutput: true,
	})
	require.NoError(t, err)
	require.Len(t, result.Manifests, 1)

	manifest := result.Manifests[0]
	require.Equal(t, []string{"id", "tenant_id", "status"}, manifest.Expected.FinalSchemaColumns)
	require.Equal(t, 1999, manifest.Expected.FinalRowCount)
	require.NotEqual(t, 2997, manifest.Expected.FinalRowCount)
	require.Contains(t, manifest.Expected.SelectedRows, "2")
	require.NotContains(t, manifest.Expected.SelectedRows, "3")

	baseColumns := baselineColumns()
	finalColumns := columnsForScenario("DDL-V1-027", baseColumns)
	snapshotRows := makeRows(140, profileConfigs[ProfileE2E].SnapshotRows, baseColumns, "src_test", "orders", "tenant_test", false)
	cdcRows := makeRows(10140, profileConfigs[ProfileE2E].CDCInserts, finalColumns, "src_test", "orders", "tenant_test", true)
	expectedRows := expectedRowsForScenario("DDL-V1-027", snapshotRows, cdcRows)
	require.Len(t, expectedRows, manifest.Expected.FinalRowCount)
	require.False(t, hasRowID(expectedRows, "1"))
	require.True(t, hasRowID(expectedRows, "2"))
	require.False(t, hasRowID(expectedRows, "3"))

	var ddlEvent TimelineEvent
	for _, event := range manifest.Timeline {
		if event.Kind == "ddl" {
			ddlEvent = event
			break
		}
	}
	require.NotEmpty(t, ddlEvent.Files)
	require.Contains(t, ddlEvent.SQL, "DROP TABLE orders")
	require.Contains(t, ddlEvent.SQL, "CREATE TABLE orders")

	data, err := os.ReadFile(filepath.Join(dir, ddlEvent.Files[0]))
	require.NoError(t, err)
	var tableDef TableDefinition
	require.NoError(t, json.Unmarshal(data, &tableDef))
	require.Equal(t, actionCreateTable, tableDef.Type)
	require.Equal(t, []string{"id", "tenant_id", "status"}, columnNames(tableDef.Columns))
}

func TestGenerateSelectedRowsMatchFinalTableColumns(t *testing.T) {
	dir := t.TempDir()
	result, err := Generate(Options{
		OutputDir:   dir,
		Seed:        7,
		ScenarioIDs: []string{"DDL-V1-001"},
		Tenants: []TenantTask{{
			TenantID:          "tenant_test",
			TaskID:            "task_test",
			SourceSchema:      "src_test",
			DestinationSchema: "DW_TEST",
			StoragePrefix:     "tenant_test/task_test",
			CheckpointPrefix:  "tenant_test/task_test/checkpoints",
		}},
		CleanOutput: true,
	})
	require.NoError(t, err)
	require.Len(t, result.Manifests, 1)

	manifest := result.Manifests[0]
	require.Contains(t, manifest.Expected.SelectedRows, "2")
	for id, row := range manifest.Expected.SelectedRows {
		require.ElementsMatch(t, manifest.Expected.FinalSchemaColumns, rowKeys(row), "selected row %s must match Snowflake final table shape", id)
		require.NotContains(t, row, "_flag")
		require.NotContains(t, row, "_schema")
		require.NotContains(t, row, "_table")
		require.NotContains(t, row, "_commit_ts")
	}
}

func TestAllPIN10ScenarioIDsAreCovered(t *testing.T) {
	ids := AllScenarioIDs()
	require.Len(t, ids, 28)
	require.Equal(t, "DDL-V1-001", ids[0])
	require.Equal(t, "DDL-V1-028", ids[len(ids)-1])
}

func rowKeys(row Row) []string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	return keys
}

func hasRowID(rows []Row, id string) bool {
	for _, row := range rows {
		if row["id"] == id {
			return true
		}
	}
	return false
}
