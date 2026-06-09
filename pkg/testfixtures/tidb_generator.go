package testfixtures

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pingcap/tiflow/pkg/hash"
)

const (
	ProfileSmoke = "smoke"
	ProfileE2E   = "e2e"

	defaultTableDefinitionVersion = 1
	defaultFileIndexWidth         = 20

	actionNone                   = 0
	actionCreateTable            = 3
	actionDropTable              = 4
	actionAddColumn              = 5
	actionDropColumn             = 6
	actionAddIndex               = 7
	actionTruncateTable          = 11
	actionModifyColumn           = 12
	actionSetDefaultValue        = 15
	actionAlterTablePartitioning = 19
	actionRenameTables           = 47
)

type Options struct {
	OutputDir   string
	Seed        int64
	Profile     string
	ScenarioIDs []string
	Tenants     []TenantTask
	CleanOutput bool
}

type TenantTask struct {
	TenantID          string `json:"tenant_id"`
	TaskID            string `json:"task_id"`
	SourceSchema      string `json:"source_schema"`
	DestinationSchema string `json:"destination_schema"`
	StoragePrefix     string `json:"storage_prefix"`
	CheckpointPrefix  string `json:"checkpoint_prefix"`
}

type ScenarioSpec struct {
	ID          string   `json:"id"`
	Status      string   `json:"status"`
	Description string   `json:"description"`
	DDL         []string `json:"ddl"`
}

type TimelineEvent struct {
	Kind         string   `json:"kind"`
	Table        string   `json:"table"`
	TableVersion uint64   `json:"table_version,omitempty"`
	SQL          string   `json:"sql,omitempty"`
	Files        []string `json:"files,omitempty"`
}

type ValidationManifest struct {
	ScenarioID     string          `json:"scenario_id"`
	Status         string          `json:"status"`
	Seed           int64           `json:"seed"`
	Profile        string          `json:"profile"`
	Tenant         TenantTask      `json:"tenant"`
	SourceSchema   string          `json:"source_schema"`
	Table          string          `json:"table"`
	SnapshotFiles  []string        `json:"snapshot_files"`
	IncrementFiles []string        `json:"increment_files"`
	MappingFile    string          `json:"mapping_file"`
	Timeline       []TimelineEvent `json:"timeline"`
	Expected       ExpectedOutcome `json:"expected"`
	Notes          []string        `json:"notes,omitempty"`
}

type ExpectedOutcome struct {
	FinalSchemaColumns []string       `json:"final_schema_columns"`
	FinalRowCount      int            `json:"final_row_count"`
	Checksum           string         `json:"checksum"`
	SelectedRows       map[string]Row `json:"selected_rows"`
	ExpectedError      string         `json:"expected_error,omitempty"`
}

type Row map[string]string

type TableCol struct {
	ID        string      `json:"ColumnId,omitempty"`
	Name      string      `json:"ColumnName"`
	Tp        string      `json:"ColumnType"`
	Default   interface{} `json:"ColumnDefault,omitempty"`
	Precision string      `json:"ColumnPrecision,omitempty"`
	Scale     string      `json:"ColumnScale,omitempty"`
	Nullable  string      `json:"ColumnNullable,omitempty"`
	IsPK      string      `json:"ColumnIsPk,omitempty"`
}

type TableDefinition struct {
	Table        string     `json:"Table"`
	Schema       string     `json:"Schema"`
	Version      uint64     `json:"Version"`
	TableVersion uint64     `json:"TableVersion"`
	Query        string     `json:"Query"`
	Type         int        `json:"Type"`
	Columns      []TableCol `json:"TableColumns"`
	TotalColumns int        `json:"TableColumnsTotal"`
}

type tableDefWithoutQuery struct {
	Table        string     `json:"Table"`
	Schema       string     `json:"Schema"`
	Version      uint64     `json:"Version"`
	Columns      []TableCol `json:"TableColumns"`
	TotalColumns int        `json:"TableColumnsTotal"`
}

type Result struct {
	OutputDir string               `json:"output_dir"`
	Manifests []ValidationManifest `json:"manifests"`
}

type profileConfig struct {
	SnapshotRows int
	CDCInserts   int
	FlushRows    int
}

var profileConfigs = map[string]profileConfig{
	ProfileSmoke: {SnapshotRows: 12, CDCInserts: 18, FlushRows: 5},
	ProfileE2E:   {SnapshotRows: 1000, CDCInserts: 2000, FlushRows: 500},
}

func Generate(options Options) (*Result, error) {
	options = normalizeOptions(options)
	cfg, ok := profileConfigs[options.Profile]
	if !ok {
		return nil, fmt.Errorf("unknown fixture profile %q", options.Profile)
	}
	if options.OutputDir == "" {
		return nil, fmt.Errorf("output directory is required")
	}
	if options.CleanOutput {
		for _, name := range []string{"snapshot", "increment", "manifests", "mappings"} {
			if err := os.RemoveAll(filepath.Join(options.OutputDir, name)); err != nil {
				return nil, fmt.Errorf("clean %s: %w", name, err)
			}
		}
	}

	specs := scenarioSpecMap()
	var manifests []ValidationManifest
	for _, tenant := range options.Tenants {
		if err := writeJSON(filepath.Join(options.OutputDir, "mappings", mappingFileName(tenant)), tenant); err != nil {
			return nil, err
		}
		for _, scenarioID := range options.ScenarioIDs {
			spec, ok := specs[scenarioID]
			if !ok {
				return nil, fmt.Errorf("unknown scenario %q", scenarioID)
			}
			manifest, err := generateScenario(options, cfg, tenant, spec)
			if err != nil {
				return nil, err
			}
			manifests = append(manifests, manifest)
		}
	}
	sort.Slice(manifests, func(i, j int) bool {
		if manifests[i].Tenant.TenantID == manifests[j].Tenant.TenantID {
			return manifests[i].ScenarioID < manifests[j].ScenarioID
		}
		return manifests[i].Tenant.TenantID < manifests[j].Tenant.TenantID
	})

	result := &Result{OutputDir: options.OutputDir, Manifests: manifests}
	if err := writeJSON(filepath.Join(options.OutputDir, "manifests", "manifest.json"), result); err != nil {
		return nil, err
	}
	return result, nil
}

func normalizeOptions(options Options) Options {
	if options.Profile == "" {
		options.Profile = ProfileSmoke
	}
	if options.Seed == 0 {
		options.Seed = 1
	}
	if len(options.ScenarioIDs) == 0 {
		options.ScenarioIDs = AllScenarioIDs()
	}
	if len(options.Tenants) == 0 {
		options.Tenants = []TenantTask{
			{
				TenantID:          "tenant_a",
				TaskID:            "task_orders",
				SourceSchema:      "tidb2dw_tenant_a_task_orders",
				DestinationSchema: "DW_TENANT_A_TASK_ORDERS",
				StoragePrefix:     "tenant_a/task_orders",
				CheckpointPrefix:  "tenant_a/task_orders/checkpoints",
			},
			{
				TenantID:          "tenant_b",
				TaskID:            "task_orders",
				SourceSchema:      "tidb2dw_tenant_b_task_orders",
				DestinationSchema: "DW_TENANT_B_TASK_ORDERS",
				StoragePrefix:     "tenant_b/task_orders",
				CheckpointPrefix:  "tenant_b/task_orders/checkpoints",
			},
		}
	}
	return options
}

func AllScenarioIDs() []string {
	specs := scenarioSpecMap()
	ids := make([]string, 0, len(specs))
	for id := range specs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func generateScenario(options Options, cfg profileConfig, tenant TenantTask, spec ScenarioSpec) (ValidationManifest, error) {
	table := "orders"
	if spec.ID == "DDL-V1-002" {
		table = "refunds"
	}
	scenarioPrefix := filepath.Join(safeName(spec.ID), safeName(tenant.TenantID), safeName(tenant.TaskID))
	sourceSchema := tenant.SourceSchema
	if spec.ID == "DDL-V1-028" {
		sourceSchema = fmt.Sprintf("%s_%s", tenant.SourceSchema, safeName(spec.ID))
	}

	baseColumns := baselineColumns()
	finalColumns := columnsForScenario(spec.ID, baseColumns)
	snapshotRows := makeRows(options.Seed, cfg.SnapshotRows, baseColumns, sourceSchema, table, tenant.TenantID, false)
	cdcRows := makeRows(options.Seed+10000, cfg.CDCInserts, finalColumns, sourceSchema, table, tenant.TenantID, true)

	snapshotRel := filepath.ToSlash(filepath.Join("snapshot", scenarioPrefix, fmt.Sprintf("%s.%s.000000000000001.csv", sourceSchema, table)))
	if err := writeRows(filepath.Join(options.OutputDir, snapshotRel), baseColumns, snapshotRows, false); err != nil {
		return ValidationManifest{}, err
	}
	loadInfoRel := filepath.ToSlash(filepath.Join("snapshot", scenarioPrefix, fmt.Sprintf("%s.%s.loadinfo", sourceSchema, table)))
	if err := writeText(filepath.Join(options.OutputDir, loadInfoRel), "deterministic fixture snapshot checkpoint\n"); err != nil {
		return ValidationManifest{}, err
	}

	var incrementFiles []string
	var timeline []TimelineEvent
	initialDef := tableDefinition(sourceSchema, table, 1000+uint64(stableOffset(spec.ID, tenant.TenantID)), "", actionNone, baseColumns)
	initialSchemaRel, err := writeSchema(options.OutputDir, scenarioPrefix, initialDef)
	if err != nil {
		return ValidationManifest{}, err
	}
	incrementFiles = append(incrementFiles, initialSchemaRel)
	timeline = append(timeline, TimelineEvent{Kind: "schema", Table: table, TableVersion: initialDef.TableVersion, Files: []string{initialSchemaRel}})

	if len(spec.DDL) > 0 {
		nextDef := tableDefinition(sourceSchema, table, initialDef.TableVersion+10, strings.Join(spec.DDL, "\n"), actionForScenario(spec.ID), finalColumns)
		nextSchemaRel, err := writeSchema(options.OutputDir, scenarioPrefix, nextDef)
		if err != nil {
			return ValidationManifest{}, err
		}
		incrementFiles = append(incrementFiles, nextSchemaRel)
		timeline = append(timeline, TimelineEvent{Kind: "ddl", Table: table, TableVersion: nextDef.TableVersion, SQL: nextDef.Query, Files: []string{nextSchemaRel}})
	}

	dmlVersion := initialDef.TableVersion
	if len(spec.DDL) > 0 {
		dmlVersion = initialDef.TableVersion + 10
	}
	dmlFiles, err := writeDMLBatches(options.OutputDir, scenarioPrefix, sourceSchema, table, dmlVersion, finalColumns, cdcRows, cfg.FlushRows)
	if err != nil {
		return ValidationManifest{}, err
	}
	incrementFiles = append(incrementFiles, dmlFiles...)
	timeline = append(timeline, TimelineEvent{Kind: "dml", Table: table, TableVersion: dmlVersion, Files: dmlFiles})

	expectedRows := expectedRowsForScenario(spec.ID, snapshotRows, cdcRows)
	projectedExpectedRows := projectRows(expectedRows, finalColumns)
	expected := ExpectedOutcome{
		FinalSchemaColumns: columnNames(finalColumns),
		FinalRowCount:      len(projectedExpectedRows),
		Checksum:           checksumRows(projectedExpectedRows, finalColumns),
		SelectedRows:       selectedRows(projectedExpectedRows, finalColumns),
	}
	if spec.Status == "unsupported" {
		expected.ExpectedError = unsupportedError(spec.ID)
	}
	if spec.Status == "deferred" {
		expected.ExpectedError = "deferred by ddl coverage contract v1"
	}

	manifest := ValidationManifest{
		ScenarioID:     spec.ID,
		Status:         spec.Status,
		Seed:           options.Seed,
		Profile:        options.Profile,
		Tenant:         tenant,
		SourceSchema:   sourceSchema,
		Table:          table,
		SnapshotFiles:  []string{snapshotRel, loadInfoRel},
		IncrementFiles: incrementFiles,
		MappingFile:    filepath.ToSlash(filepath.Join("mappings", mappingFileName(tenant))),
		Timeline:       timeline,
		Expected:       expected,
	}
	manifestPath := filepath.Join(options.OutputDir, "manifests", safeName(tenant.TenantID), fmt.Sprintf("%s.json", safeName(spec.ID)))
	if err := writeJSON(manifestPath, manifest); err != nil {
		return ValidationManifest{}, err
	}
	return manifest, nil
}

func baselineColumns() []TableCol {
	return []TableCol{
		col("1", "id", "bigint", "", "", "false", "true", nil),
		col("2", "tenant_id", "bigint", "", "", "false", "", nil),
		col("3", "status", "varchar", "32", "", "true", "", nil),
		col("4", "amount", "decimal", "18", "2", "true", "", nil),
		col("5", "created_at", "datetime", "", "3", "true", "", nil),
	}
}

func columnsForScenario(id string, base []TableCol) []TableCol {
	cols := copyColumns(base)
	switch id {
	case "DDL-V1-005", "DDL-V1-023":
		return append(cols, col("6", "coupon_code", "varchar", "64", "", "true", "", nil))
	case "DDL-V1-006":
		return append(cols, col("6", "source", "varchar", "16", "", "false", "", "web"))
	case "DDL-V1-007":
		return withoutColumn(cols, "status")
	case "DDL-V1-008":
		for i := range cols {
			if cols[i].Name == "amount" {
				cols[i].Name = "gross_amount"
			}
		}
	case "DDL-V1-009":
		return replaceColumn(cols, col("3", "status", "varchar", "128", "", "true", "", nil))
	case "DDL-V1-010":
		return replaceColumn(cols, col("4", "amount", "decimal", "20", "4", "true", "", nil))
	case "DDL-V1-012":
		return replaceColumn(cols, col("3", "status", "varchar", "32", "", "false", "", nil))
	case "DDL-V1-014":
		return replaceColumn(cols, col("3", "status", "varchar", "32", "", "true", "", nil))
	case "DDL-V1-026":
		return append(cols, col("6", "payload", "mediumblob", "16777215", "", "true", "", nil))
	case "DDL-V1-027":
		return selectColumns(cols, "id", "tenant_id", "status")
	}
	return cols
}

func actionForScenario(id string) int {
	switch id {
	case "DDL-V1-002", "DDL-V1-027":
		return actionCreateTable
	case "DDL-V1-003":
		return actionDropTable
	case "DDL-V1-004":
		return actionRenameTables
	case "DDL-V1-005", "DDL-V1-006", "DDL-V1-026":
		return actionAddColumn
	case "DDL-V1-007":
		return actionDropColumn
	case "DDL-V1-008":
		return actionModifyColumn
	case "DDL-V1-009", "DDL-V1-010", "DDL-V1-011", "DDL-V1-012", "DDL-V1-013", "DDL-V1-014":
		return actionModifyColumn
	case "DDL-V1-015", "DDL-V1-016":
		return actionSetDefaultValue
	case "DDL-V1-017", "DDL-V1-018", "DDL-V1-019":
		return actionAddIndex
	case "DDL-V1-020":
		return actionTruncateTable
	case "DDL-V1-021", "DDL-V1-022":
		return actionAlterTablePartitioning
	default:
		return actionNone
	}
}

func tableDefinition(schema, table string, version uint64, query string, action int, columns []TableCol) TableDefinition {
	return TableDefinition{
		Table:        table,
		Schema:       schema,
		Version:      defaultTableDefinitionVersion,
		TableVersion: version,
		Query:        query,
		Type:         action,
		Columns:      columns,
		TotalColumns: len(columns),
	}
}

func writeSchema(outDir, scenarioPrefix string, def TableDefinition) (string, error) {
	rel, err := def.GenerateSchemaFilePath()
	if err != nil {
		return "", err
	}
	rel = filepath.ToSlash(filepath.Join("increment", scenarioPrefix, rel))
	data, err := def.MarshalWithQuery()
	if err != nil {
		return "", err
	}
	return rel, writeBytes(filepath.Join(outDir, rel), data)
}

func writeDMLBatches(outDir, scenarioPrefix, schema, table string, version uint64, columns []TableCol, rows []Row, flushRows int) ([]string, error) {
	files := make([]string, 0, (len(rows)+flushRows-1)/flushRows)
	for start, idx := 0, uint64(1); start < len(rows); start, idx = start+flushRows, idx+1 {
		end := start + flushRows
		if end > len(rows) {
			end = len(rows)
		}
		rel := filepath.ToSlash(filepath.Join("increment", scenarioPrefix, generateDMLFilePath(schema, table, version, "2026-06-07", idx)))
		if err := writeRows(filepath.Join(outDir, rel), columns, rows[start:end], true); err != nil {
			return nil, err
		}
		files = append(files, rel)
	}
	return files, nil
}

func makeRows(seed int64, count int, columns []TableCol, schema, table, tenantID string, cdc bool) []Row {
	rows := make([]Row, 0, count)
	base := int(seed % 997)
	for i := 1; i <= count; i++ {
		id := i
		if cdc {
			id += 100000
		}
		row := Row{
			"_flag":      "I",
			"_schema":    schema,
			"_table":     table,
			"_commit_ts": fmt.Sprintf("%d", 450000000000000000+seed+int64(i)),
		}
		for _, column := range columns {
			switch column.Name {
			case "id":
				row[column.Name] = fmt.Sprintf("%d", id)
			case "tenant_id":
				row[column.Name] = fmt.Sprintf("%d", stableOffset(tenantID, "tenant")%10000)
			case "status":
				if i%7 == 0 {
					row[column.Name] = `\N`
				} else {
					row[column.Name] = fmt.Sprintf("status_%02d", (base+i)%23)
				}
			case "amount", "gross_amount":
				row[column.Name] = fmt.Sprintf("%d.%02d", 100+(base+i)%500, (base+i)%100)
			case "created_at":
				row[column.Name] = fmt.Sprintf("2026-06-%02d 12:%02d:00.000", 1+(i%27), i%60)
			case "coupon_code":
				if i%3 == 0 {
					row[column.Name] = `\N`
				} else {
					row[column.Name] = fmt.Sprintf("C%04d", base+i)
				}
			case "source":
				row[column.Name] = "web"
			case "payload":
				row[column.Name] = fmt.Sprintf("payload_%d", base+i)
			default:
				row[column.Name] = fmt.Sprintf("%s_%d", column.Name, base+i)
			}
		}
		rows = append(rows, row)
	}
	if cdc && len(rows) >= 2 {
		rows[0]["id"] = "2"
		rows[0]["_flag"] = "U"
		rows[1]["id"] = "3"
		rows[1]["_flag"] = "D"
	}
	return rows
}

func writeRows(path string, columns []TableCol, rows []Row, cdc bool) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	for _, row := range rows {
		record := make([]string, 0, len(columns)+4)
		if cdc {
			record = append(record, row["_flag"], row["_schema"], row["_table"], row["_commit_ts"])
		}
		for _, column := range columns {
			record = append(record, row[column.Name])
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func expectedRowsForScenario(id string, snapshotRows, cdcRows []Row) []Row {
	if id == "DDL-V1-003" {
		return nil
	}
	if id == "DDL-V1-020" || id == "DDL-V1-027" {
		return mergeRows(nil, cdcRows)
	}
	return mergeRows(snapshotRows, cdcRows)
}

func mergeRows(snapshotRows, cdcRows []Row) []Row {
	byID := make(map[string]Row)
	for _, row := range snapshotRows {
		byID[row["id"]] = row
	}
	for _, row := range cdcRows {
		if row["_flag"] == "D" {
			delete(byID, row["id"])
			continue
		}
		byID[row["id"]] = row
	}
	rows := make([]Row, 0, len(byID))
	for _, row := range byID {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i]["id"] < rows[j]["id"] })
	return rows
}

func checksumRows(rows []Row, columns []TableCol) string {
	hash := fnv.New64a()
	sort.Slice(rows, func(i, j int) bool { return rows[i]["id"] < rows[j]["id"] })
	for _, row := range rows {
		for _, column := range columns {
			hash.Write([]byte(column.Name))
			hash.Write([]byte("="))
			hash.Write([]byte(row[column.Name]))
			hash.Write([]byte(";"))
		}
	}
	return fmt.Sprintf("fnv64a:%016x", hash.Sum64())
}

func selectedRows(rows []Row, columns []TableCol) map[string]Row {
	selected := make(map[string]Row)
	for _, row := range rows {
		if row["id"] == "2" || row["id"] == "3" || row["id"] == "100001" {
			selected[row["id"]] = projectRow(row, columns)
		}
	}
	return selected
}

func projectRows(rows []Row, columns []TableCol) []Row {
	projected := make([]Row, 0, len(rows))
	for _, row := range rows {
		projected = append(projected, projectRow(row, columns))
	}
	return projected
}

func projectRow(row Row, columns []TableCol) Row {
	projected := make(Row, len(columns))
	for _, column := range columns {
		value, ok := row[column.Name]
		if !ok {
			value = `\N`
		}
		projected[column.Name] = value
	}
	return projected
}

func writeJSON(path string, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeBytes(path, data)
}

func writeText(path, value string) error {
	return writeBytes(path, []byte(value))
}

func writeBytes(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (t TableDefinition) MarshalWithQuery() ([]byte, error) {
	return json.MarshalIndent(t, "", "    ")
}

func (t TableDefinition) GenerateSchemaFilePath() (string, error) {
	checksum, err := t.Sum32()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/meta/schema_%d_%d.json", t.Schema, t.Table, t.TableVersion, checksum), nil
}

func (t TableDefinition) Sum32() (uint32, error) {
	columns := make([]TableCol, len(t.Columns))
	copy(columns, t.Columns)
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})
	defWithoutQuery := tableDefWithoutQuery{
		Table:        t.Table,
		Schema:       t.Schema,
		Version:      0,
		Columns:      columns,
		TotalColumns: t.TotalColumns,
	}
	data, err := json.MarshalIndent(defWithoutQuery, "", "    ")
	if err != nil {
		return 0, err
	}
	hasher := hash.NewPositionInertia()
	hasher.Write(data)
	return hasher.Sum32(), nil
}

func generateDMLFilePath(schema, table string, version uint64, date string, idx uint64) string {
	return fmt.Sprintf("%s/%s/%d/%s/CDC%0*d.csv", schema, table, version, date, defaultFileIndexWidth, idx)
}

func col(id, name, tp, precision, scale, nullable, isPK string, defaultValue interface{}) TableCol {
	return TableCol{ID: id, Name: name, Tp: tp, Precision: precision, Scale: scale, Nullable: nullable, IsPK: isPK, Default: defaultValue}
}

func copyColumns(columns []TableCol) []TableCol {
	copied := make([]TableCol, len(columns))
	copy(copied, columns)
	return copied
}

func withoutColumn(columns []TableCol, name string) []TableCol {
	filtered := make([]TableCol, 0, len(columns))
	for _, column := range columns {
		if column.Name != name {
			filtered = append(filtered, column)
		}
	}
	return filtered
}

func replaceColumn(columns []TableCol, replacement TableCol) []TableCol {
	for i := range columns {
		if columns[i].ID == replacement.ID {
			columns[i] = replacement
		}
	}
	return columns
}

func selectColumns(columns []TableCol, names ...string) []TableCol {
	byName := make(map[string]TableCol, len(columns))
	for _, column := range columns {
		byName[column.Name] = column
	}
	selected := make([]TableCol, 0, len(names))
	for _, name := range names {
		if column, ok := byName[name]; ok {
			selected = append(selected, column)
		}
	}
	return selected
}

func columnNames(columns []TableCol) []string {
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	return names
}

func mappingFileName(tenant TenantTask) string {
	return fmt.Sprintf("%s_%s.json", safeName(tenant.TenantID), safeName(tenant.TaskID))
}

func safeName(value string) string {
	value = strings.ToLower(value)
	value = strings.ReplaceAll(value, "-", "_")
	value = strings.ReplaceAll(value, ".", "_")
	return value
}

func stableOffset(values ...string) int {
	hash := fnv.New32a()
	for _, value := range values {
		hash.Write([]byte(value))
		hash.Write([]byte{0})
	}
	return int(hash.Sum32() % 100000)
}

func unsupportedError(id string) string {
	switch id {
	case "DDL-V1-011":
		return "unsupported incompatible type narrowing"
	case "DDL-V1-013":
		return "not-null violation"
	case "DDL-V1-024":
		return "ddl arrived before dependent dml"
	case "DDL-V1-025":
		return "duplicate or out-of-order ddl"
	case "DDL-V1-026":
		return "unsupported data type"
	default:
		return "unsupported ddl scenario"
	}
}
