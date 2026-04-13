package icebergconsumer

import (
	"testing"

	sinkcloudstorage "github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

const renameColumnActionType = timodel.ActionType(78)

func TestBuildDDLDefinitionsUsesColumnID(t *testing.T) {
	prev := &sinkiceberg.TableVersion{
		SchemaName:      "test",
		TableName:       "users",
		MetadataVersion: 1,
		Columns: []sinkiceberg.Column{
			{
				ID:   101,
				Name: "name",
				OriginalTableCol: &sinkcloudstorage.TableCol{
					ID:   "101",
					Name: "name",
					Tp:   "VARCHAR",
				},
			},
		},
	}
	curr := &sinkiceberg.TableVersion{
		SchemaName:      "test",
		TableName:       "users",
		MetadataVersion: 2,
		Columns: []sinkiceberg.Column{
			{
				ID:   101,
				Name: "display_name",
				OriginalTableCol: &sinkcloudstorage.TableCol{
					ID:   "101",
					Name: "display_name",
					Tp:   "VARCHAR",
				},
			},
		},
	}

	defs, err := BuildDDLDefinitions(prev, curr)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	require.Equal(t, renameColumnActionType, defs[0].Type)
	require.Contains(t, defs[0].Query, "ALTER TABLE")
	require.Contains(t, defs[0].Query, "RENAME COLUMN")
	require.Contains(t, defs[0].Query, "name")
	require.Contains(t, defs[0].Query, "display_name")
	require.Equal(t, []cloudstorage.TableCol{
		{
			ID:   "101",
			Name: "display_name",
			Tp:   "VARCHAR",
		},
	}, defs[0].Columns)
}

func TestBuildTableDefinitionConvertsOriginalTableCol(t *testing.T) {
	version := &sinkiceberg.TableVersion{
		SchemaName:      "test",
		TableName:       "users",
		MetadataVersion: 7,
		Columns: []sinkiceberg.Column{
			{
				ID:   11,
				Name: "status",
				OriginalTableCol: &sinkcloudstorage.TableCol{
					ID:        "11",
					Name:      "status",
					Tp:        "VARCHAR",
					Default:   "active",
					Precision: "32",
					Scale:     "0",
					Nullable:  "false",
					IsPK:      "true",
				},
			},
		},
	}

	def, err := BuildTableDefinition(version)
	require.NoError(t, err)
	require.Equal(t, cloudstorage.TableDefinition{
		Schema:       "test",
		Table:        "users",
		Version:      1,
		TableVersion: 7,
		Columns: []cloudstorage.TableCol{
			{
				ID:        "11",
				Name:      "status",
				Tp:        "VARCHAR",
				Default:   "active",
				Precision: "32",
				Scale:     "0",
				Nullable:  "false",
				IsPK:      "true",
			},
		},
		TotalColumns: 1,
	}, def)
}

func TestBuildTableDefinitionRejectsNilOriginalTableCol(t *testing.T) {
	_, err := BuildTableDefinition(&sinkiceberg.TableVersion{
		SchemaName:      "test",
		TableName:       "users",
		MetadataVersion: 3,
		Columns: []sinkiceberg.Column{
			{
				ID:   11,
				Name: "status",
			},
		},
	})
	require.ErrorContains(t, err, "OriginalTableCol")
}

func TestBuildDDLDefinitionsCreateTablePreservesEnumElems(t *testing.T) {
	curr := &sinkiceberg.TableVersion{
		SchemaName:      "test",
		TableName:       "users",
		MetadataVersion: 5,
		Columns: []sinkiceberg.Column{
			{
				ID:   13,
				Name: "status",
				OriginalTableCol: &sinkcloudstorage.TableCol{
					ID:    "13",
					Name:  "status",
					Tp:    "ENUM",
					Elems: []string{"open", "closed"},
				},
			},
		},
	}

	defs, err := BuildDDLDefinitions(nil, curr)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	require.Equal(t, "ENUM('open','closed')", defs[0].Columns[0].Tp)
	require.Contains(t, defs[0].Query, "`status` ENUM('open','closed')")
}

func TestTypeDefinitionPreservesScaleOnlyTemporalTypes(t *testing.T) {
	require.Equal(t, "DATETIME(6)", typeDefinition(cloudstorage.TableCol{
		Tp:    "DATETIME",
		Scale: "6",
	}))
}
