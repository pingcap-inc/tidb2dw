package metrics

import (
	"math"

	"github.com/pingcap/tidb/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	Namespace = "tidb2dw"
)

type Metrics struct {
	tableNumGauge             prometheus.Gauge
	snapshotTotalSizeCounter  *prometheus.CounterVec
	snapshotLoadedSizeCounter *prometheus.CounterVec
	incrementPeddingSizeGauge *prometheus.GaugeVec
	incrementLoadedCounter    *prometheus.CounterVec
	tableVersionsCounter      *prometheus.CounterVec
	errorCounter              *prometheus.CounterVec
}

func NewMetrics(f promutil.Factory) *Metrics {
	m := Metrics{}
	m.tableNumGauge = f.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "table_num",
			Help:      "number of replication tables",
		})
	m.snapshotTotalSizeCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "snapshot_total_size",
			Help:      "total file size of snapshot",
		}, []string{"table"})
	m.snapshotLoadedSizeCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "snapshot_loaded_size",
			Help:      "loaded file size of snapshot",
		}, []string{"table"})
	m.incrementPeddingSizeGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "increment_pedding_size",
			Help:      "pedding increment file size in object storage",
		}, []string{"table"})
	m.incrementLoadedCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "increment_loaded_count",
			Help:      "loaded increment file size",
		}, []string{"table"})
	m.tableVersionsCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "table_versions",
			Help:      "table version of each table",
		}, []string{"table"})
	m.errorCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "error_count",
			Help:      "Total error count during replicating",
		}, []string{"table"})
	return &m
}

func (m *Metrics) SetTableNum(v float64) {
	m.tableNumGauge.Set(v)
}

func (m *Metrics) RegisterTo(registry promutil.Registry) {
	registry.MustRegister(m.tableNumGauge)
	registry.MustRegister(m.snapshotTotalSizeCounter)
	registry.MustRegister(m.snapshotLoadedSizeCounter)
	registry.MustRegister(m.incrementPeddingSizeGauge)
	registry.MustRegister(m.incrementLoadedCounter)
	registry.MustRegister(m.tableVersionsCounter)
	registry.MustRegister(m.errorCounter)
}

func (m *Metrics) UnregisterFrom(registry promutil.Registry) {
	registry.Unregister(m.tableNumGauge)
	registry.Unregister(m.snapshotTotalSizeCounter)
	registry.Unregister(m.snapshotLoadedSizeCounter)
	registry.Unregister(m.incrementPeddingSizeGauge)
	registry.Unregister(m.incrementLoadedCounter)
	registry.Unregister(m.tableVersionsCounter)
	registry.Unregister(m.errorCounter)
}

// ReadCounter reports the current value of the counter for a specific table.
func ReadCounter(counterVec *prometheus.CounterVec, table string) float64 {
	if counterVec == nil {
		return math.NaN()
	}
	counter := counterVec.With(prometheus.Labels{"table": table})
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Counter.GetValue()
}

// AddCounter adds a counter for a specific table.
func AddCounter(counterVec *prometheus.CounterVec, v float64, table string) {
	if counterVec == nil {
		return
	}
	counterVec.With(prometheus.Labels{"table": table}).Add(v)
}

// ReadGauge reports the current value of the gauge for a specific table.
func ReadGauge(gaugeVec *prometheus.GaugeVec, table string) float64 {
	if gaugeVec == nil {
		return math.NaN()
	}
	gauge := gaugeVec.With(prometheus.Labels{"table": table})
	var metric dto.Metric
	if err := gauge.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Gauge.GetValue()
}

// AddGauge adds a gauge for a specific table.
func AddGauge(gaugeVec *prometheus.GaugeVec, v float64, table string) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(prometheus.Labels{"table": table}).Add(v)
}

// SubGauge subs a gauge for a specific table.
func SubGauge(gaugeVec *prometheus.GaugeVec, v float64, table string) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(prometheus.Labels{"table": table}).Sub(v)
}
