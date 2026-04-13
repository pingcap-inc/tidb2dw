package rowstage

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"path"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
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
		record := []string{
			row.Op,
			tableDef.Table,
			tableDef.Schema,
			fmt.Sprintf("%d", row.CommitTs),
		}

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

	filePath := path.Join(
		"iceberg-stage",
		strings.ReplaceAll(tableFQN, ".", "_"),
		fmt.Sprintf("%d.csv", rows[len(rows)-1].CommitTs),
	)

	if err := ext.WriteFile(context.Background(), filePath, buf.Bytes()); err != nil {
		return "", err
	}

	return filePath, nil
}
