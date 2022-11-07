package issues

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestReflectNullableString(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_nullable_string_reflect (
			  Col1 String
			, Col2 Nullable(String)
		) Engine MergeTree ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nullable_string_reflect")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	rows, err := conn.Query(ctx, "SELECT * FROM test_nullable_string_reflect")
	require.NoError(t, err)
	var (
		columnTypes = rows.ColumnTypes()
		vals        = make([]interface{}, len(columnTypes))
	)
	for i := range columnTypes {
		vals[i] = reflect.New(columnTypes[i].ScanType()).Interface()
	}
	for _, v := range vals {
		switch v := v.(type) {
		case *string:
			fmt.Println(reflect.TypeOf(v).String())
		case **string:
			fmt.Println(reflect.TypeOf(v).String())
		}
	}
}
