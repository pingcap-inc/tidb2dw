package icebergconsumer

import (
	"fmt"
	"strconv"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
)

func ToRowChanges(rows []sinkiceberg.ChangeRow) ([]coreinterfaces.RowChange, error) {
	changes := make([]coreinterfaces.RowChange, 0, len(rows))
	for _, row := range rows {
		commitTs, err := strconv.ParseUint(row.CommitTs, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse commit ts %q: %w", row.CommitTs, err)
		}

		changes = append(changes, coreinterfaces.RowChange{
			Op:         row.Op,
			CommitTs:   commitTs,
			CommitTime: row.CommitTime,
			Columns:    row.Columns,
		})
	}
	return changes, nil
}
