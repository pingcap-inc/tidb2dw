package metrics

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	Namespace = "tidb2dw"
)

var (
	TableNumGauge              prometheus.Gauge
	SnapshotTotalSizeCounter   *prometheus.CounterVec
	SnapshotLoadedSizeCounter  *prometheus.CounterVec
	IncrementPendingSizeGauge  *prometheus.GaugeVec
	IncrementLoadedSizeCounter *prometheus.CounterVec
	TableVersionsCounter       *prometheus.CounterVec
	ErrorCounter               *prometheus.CounterVec
)

func init() {
	TableNumGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "table_num",
			Help:      "number of replication tables",
		})
	SnapshotTotalSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "snapshot_total_size",
			Help:      "total file size of snapshot",
		}, []string{"table"})
	SnapshotLoadedSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "snapshot_loaded_size",
			Help:      "loaded file size of snapshot",
		}, []string{"table"})
	IncrementPendingSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "increment_pending_size",
			Help:      "pedding increment file size in object storage",
		}, []string{"table"})
	IncrementLoadedSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "increment_loaded_size",
			Help:      "loaded increment file size",
		}, []string{"table"})
	TableVersionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "table_versions",
			Help:      "table version of each table",
		}, []string{"table"})
	ErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "error_count",
			Help:      "Total error count during replicating",
		}, []string{"table"})

	Register()
}

func Register() {
	prometheus.MustRegister(TableNumGauge)
	prometheus.MustRegister(SnapshotTotalSizeCounter)
	prometheus.MustRegister(SnapshotLoadedSizeCounter)
	prometheus.MustRegister(IncrementPendingSizeGauge)
	prometheus.MustRegister(IncrementLoadedSizeCounter)
	prometheus.MustRegister(TableVersionsCounter)
	prometheus.MustRegister(ErrorCounter)
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
