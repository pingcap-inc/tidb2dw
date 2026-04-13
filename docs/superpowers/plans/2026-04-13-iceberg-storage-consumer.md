# Iceberg Storage Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Iceberg storage sink consumption to `tidb2dw`, with `full`, `snapshot-only`, and `incremental-only` support across Snowflake, Redshift, BigQuery, and Databricks.

**Architecture:** Keep the existing CSV storage-sink path intact and add a parallel Iceberg pipeline. The new pipeline reads TiCDC Iceberg metadata and Parquet files through `github.com/pingcap/ticdc/pkg/sink/iceberg`, converts them into row batches plus DDL steps, and sends those batches into warehouse-specific row appliers that reuse the current bulk merge logic through temporary staged files.

**Tech Stack:** Go 1.21, Cobra, TiCDC packages (`pkg/sink/iceberg`, `pkg/sink/cloudstorage`, `pkg/config`, `pkg/util`, `pkg/logger`, `api/v2`), Prometheus, warehouse SQL drivers, object storage staging.

---

### Task 1: Shared Row Model and Staging Writer

**Files:**
- Create: `pkg/coreinterfaces/row_applier.go`
- Create: `pkg/rowstage/csv.go`
- Test: `pkg/rowstage/csv_test.go`
- Modify: `go.mod`
- Modify: `pkg/utils/incr_table.go`

- [ ] **Step 1: Write the failing test**

```go
package rowstage

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestWriteIncrementFile(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	require.NoError(t, err)

	id := "1"
	name := "alice"
	filePath, err := WriteIncrementFile(extStorage, "test.users", cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
			{Name: "name", Tp: "VARCHAR"},
		},
	}, []coreinterfaces.RowChange{
		{
			Op:       "I",
			CommitTs: 101,
			Columns: map[string]*string{
				"id":   &id,
				"name": &name,
			},
		},
	})
	require.NoError(t, err)

	content, err := extStorage.ReadFile(ctx, filePath)
	require.NoError(t, err)
	require.Equal(t, "I,users,test,101,1,alice\n", string(content))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/rowstage -run TestWriteIncrementFile -count=1`
Expected: FAIL with errors such as `undefined: WriteIncrementFile` and `undefined: coreinterfaces.RowChange`.

- [ ] **Step 3: Write minimal implementation**

```go
package coreinterfaces

import "github.com/pingcap/ticdc/pkg/sink/cloudstorage"

type RowChange struct {
	Op         string
	CommitTs   uint64
	CommitTime string
	Columns    map[string]*string
}

type RowApplier interface {
	CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error
	InitSchema(columns []cloudstorage.TableCol) error
	ExecDDL(tableDef cloudstorage.TableDefinition) error
	ApplyRows(tableDef cloudstorage.TableDefinition, rows []RowChange) error
	Close()
}
```

```go
package rowstage

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"path"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
)

func WriteIncrementFile(
	ext storage.ExternalStorage,
	tableFQN string,
	tableDef cloudstorage.TableDefinition,
	rows []coreinterfaces.RowChange,
) (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	for _, row := range rows {
		record := []string{row.Op, tableDef.Table, tableDef.Schema, fmt.Sprint(row.CommitTs)}
		for _, col := range tableDef.Columns {
			value := row.Columns[col.Name]
			if value == nil {
				record = append(record, `\N`)
				continue
			}
			record = append(record, *value)
		}
		if err := writer.Write(record); err != nil {
			return "", err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	relPath := path.Join(
		"iceberg-stage",
		strings.ReplaceAll(tableFQN, ".", "_"),
		fmt.Sprintf("%d.csv", rows[len(rows)-1].CommitTs),
	)
	return relPath, ext.WriteFile(context.Background(), relPath, buf.Bytes())
}
```

```go
module github.com/pingcap-inc/tidb2dw

require github.com/pingcap/ticdc v0.0.0-00010101000000-000000000000

replace github.com/pingcap/ticdc => /Users/zhongtenghui/go/src/github.com/pingcap/ticdc/.git/wtm/feat/storage-sink-iceberg
```

```go
package utils

import "github.com/pingcap/ticdc/pkg/sink/cloudstorage"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/rowstage -run TestWriteIncrementFile -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add go.mod pkg/coreinterfaces/row_applier.go pkg/rowstage/csv.go pkg/rowstage/csv_test.go pkg/utils/incr_table.go
git commit -s -m "feat: add shared row staging primitives"
```

### Task 2: Iceberg Config and Checkpoint Persistence

**Files:**
- Create: `pkg/icebergconsumer/config.go`
- Create: `pkg/icebergconsumer/checkpoint.go`
- Test: `pkg/icebergconsumer/checkpoint_test.go`

- [ ] **Step 1: Write the failing test**

```go
package icebergconsumer

import (
	"context"
	"net/url"
	"testing"
	"time"

	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	require.NoError(t, err)

	state := Checkpoint{
		SourceID:            "iceberg://warehouse",
		Schema:              "test",
		Table:               "users",
		LastMetadataVersion: 7,
		DDLApplied:          true,
		LastDataFile:        "test/users/data/snap-7-000001.parquet",
	}

	require.NoError(t, SaveCheckpoint(extStorage, state))

	loaded, ok, err := LoadCheckpoint(extStorage, "test", "users")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, state, loaded)
}

func TestNewConfigRejectsEmptySourceURI(t *testing.T) {
	_, err := NewConfig("", 5*time.Second)
	require.ErrorContains(t, err, "iceberg source uri")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/icebergconsumer -run 'TestCheckpointRoundTrip|TestNewConfigRejectsEmptySourceURI' -count=1`
Expected: FAIL with `undefined: Checkpoint`, `undefined: SaveCheckpoint`, `undefined: LoadCheckpoint`, and `undefined: NewConfig`.

- [ ] **Step 3: Write minimal implementation**

```go
package icebergconsumer

import (
	"errors"
	"net/url"
	"time"
)

type Mode string

const (
	ModeFull            Mode = "full"
	ModeSnapshotOnly    Mode = "snapshot-only"
	ModeIncrementalOnly Mode = "incremental-only"
)

type Config struct {
	SourceURI    *url.URL
	PollInterval time.Duration
}

func NewConfig(sourceURI string, pollInterval time.Duration) (*Config, error) {
	if sourceURI == "" {
		return nil, errors.New("iceberg source uri is required")
	}
	parsed, err := url.Parse(sourceURI)
	if err != nil {
		return nil, err
	}
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	return &Config{SourceURI: parsed, PollInterval: pollInterval}, nil
}
```

```go
package icebergconsumer

import (
	"context"
	"encoding/json"
	"path"
	"net/url"

	"github.com/pingcap/tidb/br/pkg/storage"
)

type Checkpoint struct {
	SourceID            string `json:"source_id"`
	Schema              string `json:"schema"`
	Table               string `json:"table"`
	LastMetadataVersion int    `json:"last_metadata_version"`
	DDLApplied          bool   `json:"ddl_applied"`
	LastDataFile        string `json:"last_data_file"`
}

func checkpointPath(schema, table string) string {
	return path.Join("iceberg-checkpoints", url.PathEscape(schema), url.PathEscape(table)+".json")
}

func SaveCheckpoint(ext storage.ExternalStorage, checkpoint Checkpoint) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return ext.WriteFile(context.Background(), checkpointPath(checkpoint.Schema, checkpoint.Table), data)
}

func LoadCheckpoint(ext storage.ExternalStorage, schema, table string) (Checkpoint, bool, error) {
	filePath := checkpointPath(schema, table)
	exists, err := ext.FileExists(context.Background(), filePath)
	if err != nil || !exists {
		return Checkpoint{}, false, err
	}
	data, err := ext.ReadFile(context.Background(), filePath)
	if err != nil {
		return Checkpoint{}, false, err
	}
	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return Checkpoint{}, false, err
	}
	return checkpoint, true, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/icebergconsumer -run 'TestCheckpointRoundTrip|TestNewConfigRejectsEmptySourceURI' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/icebergconsumer/config.go pkg/icebergconsumer/checkpoint.go pkg/icebergconsumer/checkpoint_test.go
git commit -s -m "feat: add iceberg consumer config and checkpoints"
```

### Task 3: Iceberg Version Mapping and Row Conversion

**Files:**
- Create: `pkg/icebergconsumer/ddl.go`
- Create: `pkg/icebergconsumer/source.go`
- Test: `pkg/icebergconsumer/ddl_test.go`
- Test: `pkg/icebergconsumer/source_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package icebergconsumer

import (
	"testing"

	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestBuildDDLDefinitionsUsesColumnID(t *testing.T) {
	prev := &sinkiceberg.TableVersion{
		SchemaName:      "test",
		TableName:       "users",
		MetadataVersion: 1,
		Columns: []sinkiceberg.Column{
			{ID: 1, Name: "id", OriginalTableCol: &cloudstorage.TableCol{Name: "id", Tp: "BIGINT", IsPK: "true"}},
			{ID: 2, Name: "name", OriginalTableCol: &cloudstorage.TableCol{Name: "name", Tp: "VARCHAR"}},
		},
	}
	curr := &sinkiceberg.TableVersion{
		SchemaName:      "test",
		TableName:       "users",
		MetadataVersion: 2,
		Columns: []sinkiceberg.Column{
			{ID: 1, Name: "id", OriginalTableCol: &cloudstorage.TableCol{Name: "id", Tp: "BIGINT", IsPK: "true"}},
			{ID: 2, Name: "full_name", OriginalTableCol: &cloudstorage.TableCol{Name: "full_name", Tp: "VARCHAR"}},
		},
	}

	steps, err := BuildDDLDefinitions(prev, curr)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, byte(model.ActionRenameColumn), steps[0].Type)
	require.Contains(t, steps[0].Query, "RENAME COLUMN")
}

func TestToRowChangesParsesCommitTs(t *testing.T) {
	id := "1"
	rows, err := ToRowChanges([]sinkiceberg.ChangeRow{{
		Op:       "D",
		CommitTs: "101",
		Columns:  map[string]*string{"id": &id, "name": nil},
	}})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(101), rows[0].CommitTs)
	require.Nil(t, rows[0].Columns["name"])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/icebergconsumer -run 'TestBuildDDLDefinitionsUsesColumnID|TestToRowChangesParsesCommitTs' -count=1`
Expected: FAIL with `undefined: BuildDDLDefinitions` and `undefined: ToRowChanges`.

- [ ] **Step 3: Write minimal implementation**

```go
package icebergconsumer

import (
	"fmt"
	"sort"

	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/pkg/meta/model"
)

func BuildTableDefinition(version *sinkiceberg.TableVersion) (cloudstorage.TableDefinition, error) {
	tableDef := cloudstorage.TableDefinition{
		Schema:       version.SchemaName,
		Table:        version.TableName,
		Version:      1,
		TableVersion: uint64(version.MetadataVersion),
	}
	for _, col := range version.Columns {
		if col.OriginalTableCol == nil {
			return tableDef, fmt.Errorf("missing original column metadata for %s", col.Name)
		}
		tableDef.Columns = append(tableDef.Columns, *col.OriginalTableCol)
	}
	tableDef.TotalColumns = len(tableDef.Columns)
	return tableDef, nil
}

func BuildDDLDefinitions(prev, curr *sinkiceberg.TableVersion) ([]cloudstorage.TableDefinition, error) {
	if prev == nil {
		tableDef, err := BuildTableDefinition(curr)
		if err != nil {
			return nil, err
		}
		tableDef.Query = fmt.Sprintf("CREATE TABLE %s.%s", curr.SchemaName, curr.TableName)
		tableDef.Type = byte(model.ActionCreateTable)
		return []cloudstorage.TableDefinition{tableDef}, nil
	}

	prevByID := make(map[int]sinkiceberg.Column, len(prev.Columns))
	currByID := make(map[int]sinkiceberg.Column, len(curr.Columns))
	for _, col := range prev.Columns {
		prevByID[col.ID] = col
	}
	for _, col := range curr.Columns {
		currByID[col.ID] = col
	}

	ids := make([]int, 0, len(prevByID)+len(currByID))
	for id := range prevByID {
		ids = append(ids, id)
	}
	for id := range currByID {
		if _, ok := prevByID[id]; !ok {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)

	tableDef, err := BuildTableDefinition(curr)
	if err != nil {
		return nil, err
	}
	steps := make([]cloudstorage.TableDefinition, 0, len(ids))
	for _, id := range ids {
		prevCol, hasPrev := prevByID[id]
		currCol, hasCurr := currByID[id]
		switch {
		case hasPrev && hasCurr && prevCol.Name != currCol.Name:
			step := tableDef
			step.Type = byte(model.ActionRenameColumn)
			step.Query = fmt.Sprintf("ALTER TABLE `%s`.`%s` RENAME COLUMN `%s` TO `%s`", curr.SchemaName, curr.TableName, prevCol.Name, currCol.Name)
			steps = append(steps, step)
		case hasPrev && !hasCurr:
			step := tableDef
			step.Type = byte(model.ActionDropColumn)
			step.Query = fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP COLUMN `%s`", curr.SchemaName, curr.TableName, prevCol.Name)
			steps = append(steps, step)
		case !hasPrev && hasCurr:
			step := tableDef
			step.Type = byte(model.ActionAddColumn)
			step.Query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN `%s`", curr.SchemaName, curr.TableName, currCol.Name)
			steps = append(steps, step)
		}
	}
	return steps, nil
}
```

```go
package icebergconsumer

import (
	"strconv"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
)

func ToRowChanges(rows []sinkiceberg.ChangeRow) ([]coreinterfaces.RowChange, error) {
	result := make([]coreinterfaces.RowChange, 0, len(rows))
	for _, row := range rows {
		commitTs, err := strconv.ParseUint(row.CommitTs, 10, 64)
		if err != nil {
			return nil, err
		}
		result = append(result, coreinterfaces.RowChange{
			Op:         row.Op,
			CommitTs:   commitTs,
			CommitTime: row.CommitTime,
			Columns:    row.Columns,
		})
	}
	return result, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/icebergconsumer -run 'TestBuildDDLDefinitionsUsesColumnID|TestToRowChangesParsesCommitTs' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/icebergconsumer/ddl.go pkg/icebergconsumer/source.go pkg/icebergconsumer/ddl_test.go pkg/icebergconsumer/source_test.go
git commit -s -m "feat: map iceberg versions into ddl and row changes"
```

### Task 4: Iceberg Runner and Command Wiring

**Files:**
- Create: `pkg/icebergconsumer/runner.go`
- Test: `pkg/icebergconsumer/runner_test.go`
- Create: `cmd/source_flags.go`
- Test: `cmd/source_flags_test.go`
- Modify: `cmd/core.go`
- Modify: `cmd/snowflake.go`
- Modify: `cmd/redshift.go`
- Modify: `cmd/bigquery.go`
- Modify: `cmd/databricks.go`

- [ ] **Step 1: Write the failing tests**

```go
package icebergconsumer

import (
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

type fakeApplier struct {
	created int
	rows    int
}

func (f *fakeApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error { f.created++; return nil }
func (f *fakeApplier) InitSchema(columns []cloudstorage.TableCol) error                      { return nil }
func (f *fakeApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error                    { return nil }
func (f *fakeApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	f.rows += len(rows)
	return nil
}
func (f *fakeApplier) Close() {}

func TestRunIncrementalOnlyCreatesTableWithoutBackfill(t *testing.T) {
	source := &FakeSource{
		Tables: []VersionedTable{{
			TableDef: cloudstorage.TableDefinition{Schema: "test", Table: "users", Columns: []cloudstorage.TableCol{{Name: "id", Tp: "BIGINT", IsPK: "true"}}},
			LatestVersion: 3,
		}},
	}
	applier := &fakeApplier{}
	runner := NewRunner(source, map[string]coreinterfaces.RowApplier{"test.users": applier}, ModeIncrementalOnly)

	require.NoError(t, runner.RunOnce())
	require.Equal(t, 1, applier.created)
	require.Equal(t, 0, applier.rows)
}
```

```go
package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultSourceOptions(t *testing.T) {
	opts := defaultSourceOptions()
	require.Equal(t, SourceFormatCSV, opts.format)
	require.Equal(t, 5*time.Second, opts.icebergPollInterval)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/icebergconsumer ./cmd -run 'TestRunIncrementalOnlyCreatesTableWithoutBackfill|TestDefaultSourceOptions' -count=1`
Expected: FAIL with missing `Runner`, `FakeSource`, `VersionedTable`, `defaultSourceOptions`, and `SourceFormatCSV`.

- [ ] **Step 3: Write minimal implementation**

```go
package icebergconsumer

import (
	"fmt"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
)

type VersionedTable struct {
	TableDef      cloudstorage.TableDefinition
	LatestVersion int
}

type Source interface {
	ListTables() ([]VersionedTable, error)
}

type FakeSource struct {
	Tables []VersionedTable
}

func (f *FakeSource) ListTables() ([]VersionedTable, error) { return f.Tables, nil }

type Runner struct {
	source   Source
	appliers map[string]coreinterfaces.RowApplier
	mode     Mode
}

func NewRunner(source Source, appliers map[string]coreinterfaces.RowApplier, mode Mode) *Runner {
	return &Runner{source: source, appliers: appliers, mode: mode}
}

func (r *Runner) RunOnce() error {
	tables, err := r.source.ListTables()
	if err != nil {
		return err
	}
	for _, table := range tables {
		key := fmt.Sprintf("%s.%s", table.TableDef.Schema, table.TableDef.Table)
		applier := r.appliers[key]
		if applier == nil {
			continue
		}
		if r.mode == ModeIncrementalOnly {
			if err := applier.CreateTableFromDefinition(table.TableDef); err != nil {
				return err
			}
			continue
		}
	}
	return nil
}
```

```go
package cmd

import "time"

type SourceFormat string

const (
	SourceFormatCSV     SourceFormat = "csv"
	SourceFormatIceberg SourceFormat = "iceberg"
)

type sourceOptions struct {
	format              SourceFormat
	icebergSourceURI    string
	icebergPollInterval time.Duration
}

func defaultSourceOptions() sourceOptions {
	return sourceOptions{
		format:              SourceFormatCSV,
		icebergPollInterval: 5 * time.Second,
	}
}
```

```go
opts := defaultSourceOptions()
cmd.Flags().StringVar((*string)(&opts.format), "source-format", string(SourceFormatCSV), "source format: csv or iceberg")
cmd.Flags().StringVar(&opts.icebergSourceURI, "iceberg.source-uri", "", "TiCDC iceberg sink uri")
cmd.Flags().DurationVar(&opts.icebergPollInterval, "iceberg.poll-interval", 5*time.Second, "iceberg metadata poll interval")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/icebergconsumer ./cmd -run 'TestRunIncrementalOnlyCreatesTableWithoutBackfill|TestDefaultSourceOptions' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/icebergconsumer/runner.go pkg/icebergconsumer/runner_test.go cmd/source_flags.go cmd/source_flags_test.go cmd/core.go cmd/snowflake.go cmd/redshift.go cmd/bigquery.go cmd/databricks.go
git commit -s -m "feat: wire iceberg source mode into commands"
```

### Task 5: Snowflake Row Applier

**Files:**
- Create: `pkg/snowsql/row_applier.go`
- Test: `pkg/snowsql/row_applier_test.go`
- Modify: `pkg/snowsql/sql.go`

- [ ] **Step 1: Write the failing tests**

```go
package snowsql

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestSnowflakeRowApplierApplyRowsStagesFile(t *testing.T) {
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)
	extStorage, err := putil.GetExternalStorageFromURI(context.Background(), storageURI.String())
	require.NoError(t, err)

	var gotPath string
	applier := &SnowflakeRowApplier{
		storage: extStorage,
		loadIncrementFn: func(tableDef cloudstorage.TableDefinition, path string) error {
			gotPath = path
			return nil
		},
	}

	id := "1"
	err = applier.ApplyRows(cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
		},
	}, []coreinterfaces.RowChange{{Op: "I", CommitTs: 101, Columns: map[string]*string{"id": &id}}})
	require.NoError(t, err)
	require.NotEmpty(t, gotPath)
}
```

```go
func TestGenCreateSchemaFromDefinition(t *testing.T) {
	sql, err := GenCreateSchemaFromDefinition(cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
			{Name: "name", Tp: "VARCHAR"},
		},
	})
	require.NoError(t, err)
	require.Contains(t, sql, "CREATE OR REPLACE TABLE users")
	require.Contains(t, sql, "PRIMARY KEY (id)")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/snowsql -run 'TestSnowflakeRowApplierApplyRowsStagesFile|TestGenCreateSchemaFromDefinition' -count=1`
Expected: FAIL with missing `SnowflakeRowApplier` and `GenCreateSchemaFromDefinition`.

- [ ] **Step 3: Write minimal implementation**

```go
package snowsql

import (
	"context"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/rowstage"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type SnowflakeRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func (a *SnowflakeRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	return a.createTableFn(tableDef)
}

func (a *SnowflakeRowApplier) InitSchema(columns []cloudstorage.TableCol) error {
	return a.initSchemaFn(columns)
}

func (a *SnowflakeRowApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	return a.execDDLFn(tableDef)
}

func (a *SnowflakeRowApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	filePath, err := rowstage.WriteIncrementFile(a.storage, tableDef.Schema+"."+tableDef.Table, tableDef, rows)
	if err != nil {
		return err
	}
	return a.loadIncrementFn(tableDef, filePath)
}

func (a *SnowflakeRowApplier) Close() {
	if a.closeFn != nil {
		a.closeFn()
	}
}
```

```go
func GenCreateSchemaFromDefinition(tableDef cloudstorage.TableDefinition) (string, error) {
	columnRows := make([]string, 0, len(tableDef.Columns))
	primaryKeys := make([]string, 0, len(tableDef.Columns))
	for _, column := range tableDef.Columns {
		row, err := GetSnowflakeColumnString(column)
		if err != nil {
			return "", err
		}
		columnRows = append(columnRows, row)
		if column.IsPK == "true" {
			primaryKeys = append(primaryKeys, column.Name)
		}
	}
	if len(primaryKeys) > 0 {
		columnRows = append(columnRows, "PRIMARY KEY ("+strings.Join(primaryKeys, ", ")+")")
	}
	return "CREATE OR REPLACE TABLE " + tableDef.Table + " (\n    " + strings.Join(columnRows, ",\n    ") + "\n)", nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/snowsql -run 'TestSnowflakeRowApplierApplyRowsStagesFile|TestGenCreateSchemaFromDefinition' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/snowsql/row_applier.go pkg/snowsql/row_applier_test.go pkg/snowsql/sql.go
git commit -s -m "feat: add snowflake iceberg row applier"
```

### Task 6: Redshift Row Applier

**Files:**
- Create: `pkg/redshiftsql/row_applier.go`
- Test: `pkg/redshiftsql/row_applier_test.go`
- Modify: `pkg/redshiftsql/sql.go`

- [ ] **Step 1: Write the failing tests**

```go
package redshiftsql

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestRedshiftRowApplierApplyRowsStagesFile(t *testing.T) {
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)
	extStorage, err := putil.GetExternalStorageFromURI(context.Background(), storageURI.String())
	require.NoError(t, err)

	var gotPath string
	applier := &RedshiftRowApplier{
		storage: extStorage,
		loadIncrementFn: func(tableDef cloudstorage.TableDefinition, path string) error {
			gotPath = path
			return nil
		},
	}

	id := "1"
	err = applier.ApplyRows(cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
		},
	}, []coreinterfaces.RowChange{{Op: "D", CommitTs: 101, Columns: map[string]*string{"id": &id}}})
	require.NoError(t, err)
	require.NotEmpty(t, gotPath)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/redshiftsql -run TestRedshiftRowApplierApplyRowsStagesFile -count=1`
Expected: FAIL with missing `RedshiftRowApplier`.

- [ ] **Step 3: Write minimal implementation**

```go
package redshiftsql

import (
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/rowstage"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type RedshiftRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func (a *RedshiftRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	return a.createTableFn(tableDef)
}

func (a *RedshiftRowApplier) InitSchema(columns []cloudstorage.TableCol) error { return a.initSchemaFn(columns) }
func (a *RedshiftRowApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	return a.execDDLFn(tableDef)
}
func (a *RedshiftRowApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	filePath, err := rowstage.WriteIncrementFile(a.storage, tableDef.Schema+"."+tableDef.Table, tableDef, rows)
	if err != nil {
		return err
	}
	return a.loadIncrementFn(tableDef, filePath)
}
func (a *RedshiftRowApplier) Close() {
	if a.closeFn != nil {
		a.closeFn()
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/redshiftsql -run TestRedshiftRowApplierApplyRowsStagesFile -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/redshiftsql/row_applier.go pkg/redshiftsql/row_applier_test.go pkg/redshiftsql/sql.go
git commit -s -m "feat: add redshift iceberg row applier"
```

### Task 7: BigQuery Row Applier

**Files:**
- Create: `pkg/bigquerysql/row_applier.go`
- Test: `pkg/bigquerysql/row_applier_test.go`
- Modify: `pkg/bigquerysql/sql.go`
- Modify: `pkg/bigquerysql/connector.go`

- [ ] **Step 1: Write the failing tests**

```go
package bigquerysql

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestBigQueryRowApplierApplyRowsStagesFile(t *testing.T) {
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)
	extStorage, err := putil.GetExternalStorageFromURI(context.Background(), storageURI.String())
	require.NoError(t, err)

	var gotPath string
	applier := &BigQueryRowApplier{
		storage: extStorage,
		loadIncrementFn: func(tableDef cloudstorage.TableDefinition, path string) error {
			gotPath = path
			return nil
		},
	}

	id := "1"
	err = applier.ApplyRows(cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
		},
	}, []coreinterfaces.RowChange{{Op: "I", CommitTs: 101, Columns: map[string]*string{"id": &id}}})
	require.NoError(t, err)
	require.NotEmpty(t, gotPath)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/bigquerysql -run TestBigQueryRowApplierApplyRowsStagesFile -count=1`
Expected: FAIL with missing `BigQueryRowApplier`.

- [ ] **Step 3: Write minimal implementation**

```go
package bigquerysql

import (
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/rowstage"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type BigQueryRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func (a *BigQueryRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	return a.createTableFn(tableDef)
}

func (a *BigQueryRowApplier) InitSchema(columns []cloudstorage.TableCol) error { return a.initSchemaFn(columns) }
func (a *BigQueryRowApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	return a.execDDLFn(tableDef)
}
func (a *BigQueryRowApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	filePath, err := rowstage.WriteIncrementFile(a.storage, tableDef.Schema+"."+tableDef.Table, tableDef, rows)
	if err != nil {
		return err
	}
	return a.loadIncrementFn(tableDef, filePath)
}
func (a *BigQueryRowApplier) Close() {
	if a.closeFn != nil {
		a.closeFn()
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/bigquerysql -run TestBigQueryRowApplierApplyRowsStagesFile -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/bigquerysql/row_applier.go pkg/bigquerysql/row_applier_test.go pkg/bigquerysql/sql.go pkg/bigquerysql/connector.go
git commit -s -m "feat: add bigquery iceberg row applier"
```

### Task 8: Databricks Row Applier

**Files:**
- Create: `pkg/databrickssql/row_applier.go`
- Test: `pkg/databrickssql/row_applier_test.go`
- Modify: `pkg/databrickssql/sql.go`

- [ ] **Step 1: Write the failing tests**

```go
package databrickssql

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestDatabricksRowApplierApplyRowsStagesFile(t *testing.T) {
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)
	extStorage, err := putil.GetExternalStorageFromURI(context.Background(), storageURI.String())
	require.NoError(t, err)

	var gotPath string
	applier := &DatabricksRowApplier{
		storage: extStorage,
		loadIncrementFn: func(tableDef cloudstorage.TableDefinition, path string) error {
			gotPath = path
			return nil
		},
	}

	id := "1"
	err = applier.ApplyRows(cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
		},
	}, []coreinterfaces.RowChange{{Op: "U", CommitTs: 101, Columns: map[string]*string{"id": &id}}})
	require.NoError(t, err)
	require.NotEmpty(t, gotPath)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/databrickssql -run TestDatabricksRowApplierApplyRowsStagesFile -count=1`
Expected: FAIL with missing `DatabricksRowApplier`.

- [ ] **Step 3: Write minimal implementation**

```go
package databrickssql

import (
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/rowstage"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type DatabricksRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func (a *DatabricksRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	return a.createTableFn(tableDef)
}

func (a *DatabricksRowApplier) InitSchema(columns []cloudstorage.TableCol) error { return a.initSchemaFn(columns) }
func (a *DatabricksRowApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	return a.execDDLFn(tableDef)
}
func (a *DatabricksRowApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	filePath, err := rowstage.WriteIncrementFile(a.storage, tableDef.Schema+"."+tableDef.Table, tableDef, rows)
	if err != nil {
		return err
	}
	return a.loadIncrementFn(tableDef, filePath)
}
func (a *DatabricksRowApplier) Close() {
	if a.closeFn != nil {
		a.closeFn()
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/databrickssql -run TestDatabricksRowApplierApplyRowsStagesFile -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/databrickssql/row_applier.go pkg/databrickssql/row_applier_test.go pkg/databrickssql/sql.go
git commit -s -m "feat: add databricks iceberg row applier"
```

### Task 9: TiCDC Import Migration, Metrics, Docs, and Final Verification

**Files:**
- Modify: `pkg/cdc/config.go`
- Modify: `pkg/cdc/connector.go`
- Modify: `replicate/snapshot.go`
- Modify: `replicate/increment.go`
- Modify: `pkg/coreinterfaces/connector.go`
- Modify: `cmd/snowflake.go`
- Modify: `cmd/redshift.go`
- Modify: `cmd/bigquery.go`
- Modify: `cmd/databricks.go`
- Modify: `cmd/s3.go`
- Modify: `cmd/gcs.go`
- Modify: `pkg/snowsql/connector.go`
- Modify: `pkg/snowsql/sql.go`
- Modify: `pkg/snowsql/ddl.go`
- Modify: `pkg/snowsql/types.go`
- Modify: `pkg/redshiftsql/connector.go`
- Modify: `pkg/redshiftsql/sql.go`
- Modify: `pkg/redshiftsql/ddl.go`
- Modify: `pkg/redshiftsql/types.go`
- Modify: `pkg/bigquerysql/connector.go`
- Modify: `pkg/bigquerysql/sql.go`
- Modify: `pkg/bigquerysql/ddl.go`
- Modify: `pkg/bigquerysql/types.go`
- Modify: `pkg/databrickssql/connector.go`
- Modify: `pkg/databrickssql/sql.go`
- Modify: `pkg/databrickssql/ddl.go`
- Modify: `pkg/databrickssql/types.go`
- Modify: `pkg/dumpling/dump.go`
- Modify: `pkg/tidbsql/ddl.go`
- Modify: `pkg/tidbsql/ddl_test.go`
- Modify: `pkg/snowsql/ddl_test.go`
- Modify: `pkg/redshiftsql/ddl_test.go`
- Modify: `pkg/metrics/metrics.go`
- Test: `pkg/metrics/metrics_test.go`
- Modify: `README.md`
- Modify: `docs/snowflake.md`
- Modify: `docs/redshift.md`
- Modify: `docs/bigquery.md`
- Modify: `docs/databricks.md`

- [ ] **Step 1: Write the failing tests**

```go
package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIcebergVersionGaugeHelpers(t *testing.T) {
	AddGauge(IncrementPendingSizeGauge, 0, "test.users")
	AddGauge(IcebergMetadataVersionGauge, 7, "test.users")

	require.Equal(t, float64(7), ReadGauge(IcebergMetadataVersionGauge, "test.users"))
}
```

```go
func TestAddGauge(t *testing.T) {
	gaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_gauge",
		Help: "Test gauge",
	}, []string{"table"})

	AddGauge(gaugeVec, 1.0, "test_table")
	require.Equal(t, float64(1), ReadGauge(gaugeVec, "test_table"))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/metrics -run 'TestIcebergVersionGaugeHelpers|TestAddGauge' -count=1`
Expected: FAIL with missing `IcebergMetadataVersionGauge` and the current brittle `Desc().String()` assertion still failing.

- [ ] **Step 3: Write minimal implementation**

```go
package metrics

var (
	IcebergMetadataVersionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "iceberg_metadata_version",
			Help:      "last applied iceberg metadata version",
		}, []string{"table"},
	)
)

func Register() {
	prometheus.MustRegister(TableNumGauge)
	prometheus.MustRegister(SnapshotTotalSizeCounter)
	prometheus.MustRegister(SnapshotLoadedSizeCounter)
	prometheus.MustRegister(IncrementPendingSizeGauge)
	prometheus.MustRegister(IncrementLoadedSizeCounter)
	prometheus.MustRegister(TableVersionsCounter)
	prometheus.MustRegister(ErrorCounter)
	prometheus.MustRegister(IcebergMetadataVersionGauge)
}
```

```go
package cdc

import (
	apiv2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/config"
	putil "github.com/pingcap/ticdc/pkg/util"
)
```

```go
package cmd

import "github.com/pingcap/ticdc/pkg/logger"

err := logger.InitLogger(&logger.Config{
	Level: logLevel,
	File:  logFile,
})
```

```md
## Iceberg Source

`tidb2dw` now supports `--source-format=iceberg` together with `--iceberg.source-uri`.

- `--storage` stays as the writable downstream staging workspace.
- `--iceberg.source-uri` points at the TiCDC Iceberg sink.
- `snapshot-only` replays versions `1..latest`.
- `incremental-only` creates downstream tables from the latest schema and only consumes future versions.
```

- [ ] **Step 4: Run verification to confirm the change**

Run: `go test ./pkg/rowstage ./pkg/icebergconsumer ./pkg/snowsql ./pkg/redshiftsql ./pkg/bigquerysql ./pkg/databrickssql ./pkg/metrics -count=1`
Expected: PASS

Run: `go test ./... -count=1`
Expected: PASS

Run: repo grep for direct legacy module import paths
Expected: no matches

- [ ] **Step 5: Commit**

```bash
git add go.mod pkg/cdc/config.go pkg/cdc/connector.go replicate/snapshot.go replicate/increment.go pkg/coreinterfaces/connector.go cmd/snowflake.go cmd/redshift.go cmd/bigquery.go cmd/databricks.go cmd/s3.go cmd/gcs.go pkg/snowsql/connector.go pkg/snowsql/sql.go pkg/snowsql/ddl.go pkg/snowsql/types.go pkg/redshiftsql/connector.go pkg/redshiftsql/sql.go pkg/redshiftsql/ddl.go pkg/redshiftsql/types.go pkg/bigquerysql/connector.go pkg/bigquerysql/sql.go pkg/bigquerysql/ddl.go pkg/bigquerysql/types.go pkg/databrickssql/connector.go pkg/databrickssql/sql.go pkg/databrickssql/ddl.go pkg/databrickssql/types.go pkg/dumpling/dump.go pkg/tidbsql/ddl.go pkg/tidbsql/ddl_test.go pkg/snowsql/ddl_test.go pkg/redshiftsql/ddl_test.go pkg/metrics/metrics.go pkg/metrics/metrics_test.go README.md docs/snowflake.md docs/redshift.md docs/bigquery.md docs/databricks.md
git commit -s -m "feat: add iceberg storage consumer support"
```
