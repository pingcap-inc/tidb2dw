package metrics

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetricsRegistration(t *testing.T) {
	m := NewMetrics(promutil.NewDefaultFactory())
	registry := promutil.NewDefaultRegistry()
	m.RegisterTo(registry)
	m.UnregisterFrom(registry)
}

func TestReadCounter(t *testing.T) {
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_counter",
		Help: "Test counter",
	}, []string{"table"})

	// Add some values to the counter
	counterVec.With(prometheus.Labels{"table": "table1"}).Add(10)
	counterVec.With(prometheus.Labels{"table": "table2"}).Add(20)

	// Test reading the counter for a specific table
	table1Value := ReadCounter(counterVec, "table1")
	require.Equal(t, float64(10), table1Value)

	table2Value := ReadCounter(counterVec, "table2")
	require.Equal(t, float64(20), table2Value)

	// Test reading the counter for a non-existent table
	nonExistentValue := ReadCounter(nil, "non-existent")
	require.True(t, math.IsNaN(nonExistentValue))
}

func TestAddCounter(t *testing.T) {
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_counter",
		Help: "Test counter",
	}, []string{"table"})

	// Add a value to the counter for a specific table
	AddCounter(counterVec, 10, "table1")
	metricValue := ReadCounter(counterVec, "table1")
	require.Equal(t, metricValue, float64(10))

	// Add a value to the counter for a non-existent table
	AddCounter(nil, 10, "non-existent")
}

func TestReadGauge(t *testing.T) {
	gaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_gauge",
		Help: "Test gauge",
	}, []string{"table"})

	// Set some values to the gauge
	gaugeVec.With(prometheus.Labels{"table": "table1"}).Set(10)
	gaugeVec.With(prometheus.Labels{"table": "table2"}).Set(20)

	// Test reading the gauge for a specific table
	table1Value := ReadGauge(gaugeVec, "table1")
	require.Equal(t, float64(10), table1Value)

	table2Value := ReadGauge(gaugeVec, "table2")
	require.Equal(t, float64(20), table2Value)

	// Test reading the gauge for a non-existent table
	nonExistentValue := ReadGauge(nil, "non-existent")
	require.True(t, math.IsNaN(nonExistentValue))
}

func TestAddGauge(t *testing.T) {
	gaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_gauge",
		Help: "Test gauge",
	}, []string{"table"})

	AddGauge(gaugeVec, 1.0, "test_table")

	metric, err := gaugeVec.GetMetricWithLabelValues("test_table")
	require.NoError(t, err)
	require.NotNil(t, metric)
	require.Equal(t, "Desc{fqName: \"test_gauge\", help: \"Test gauge\", constLabels: {}, variableLabels: [{table <nil>}]}", metric.Desc().String())

	metricValue := ReadGauge(gaugeVec, "test_table")
	require.Equal(t, float64(1), metricValue)
}

func TestSubGauge(t *testing.T) {
	gaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_gauge",
		Help: "Test gauge",
	}, []string{"table"})

	AddGauge(gaugeVec, 1.0, "test_table")
	SubGauge(gaugeVec, 0.5, "test_table")

	metric, err := gaugeVec.GetMetricWithLabelValues("test_table")
	require.NoError(t, err)
	require.NotNil(t, metric)
	require.Equal(t, "Desc{fqName: \"test_gauge\", help: \"Test gauge\", constLabels: {}, variableLabels: [{table <nil>}]}", metric.Desc().String())

	metricValue := ReadGauge(gaugeVec, "test_table")
	require.Equal(t, float64(0.5), metricValue)
}
