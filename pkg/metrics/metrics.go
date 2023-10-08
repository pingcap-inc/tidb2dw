package metrics

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	Namespace = "tidb2dw"
)

type Metrics struct {
	TableNumGauge             prometheus.Gauge
	SnapshotTotalSizeCounter  *prometheus.CounterVec
	SnapshotLoadedSizeCounter *prometheus.CounterVec
	IncrementPeddingSizeGauge *prometheus.GaugeVec
	IncrementLoadedCounter    *prometheus.CounterVec
	TableVersionsCounter      *prometheus.CounterVec
	ErrorCounter              *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	m := Metrics{}
	m.TableNumGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "table_num",
			Help:      "number of replication tables",
		})
	m.SnapshotTotalSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "snapshot_total_size",
			Help:      "total file size of snapshot",
		}, []string{"table"})
	m.SnapshotLoadedSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "snapshot_loaded_size",
			Help:      "loaded file size of snapshot",
		}, []string{"table"})
	m.IncrementPeddingSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "increment_pedding_size",
			Help:      "pedding increment file size in object storage",
		}, []string{"table"})
	m.IncrementLoadedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "increment_loaded_count",
			Help:      "loaded increment file size",
		}, []string{"table"})
	m.TableVersionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "table_versions",
			Help:      "table version of each table",
		}, []string{"table"})
	m.ErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "error_count",
			Help:      "Total error count during replicating",
		}, []string{"table"})
	return &m
}

func (m *Metrics) SetTableNum(v int) {
	m.TableNumGauge.Set(float64(v))
}

func (m *Metrics) Register() {
	prometheus.MustRegister(m.TableNumGauge)
	prometheus.MustRegister(m.SnapshotTotalSizeCounter)
	prometheus.MustRegister(m.SnapshotLoadedSizeCounter)
	prometheus.MustRegister(m.IncrementPeddingSizeGauge)
	prometheus.MustRegister(m.IncrementLoadedCounter)
	prometheus.MustRegister(m.TableVersionsCounter)
	prometheus.MustRegister(m.ErrorCounter)
}

func (m *Metrics) Unregister() {
	prometheus.Unregister(m.TableNumGauge)
	prometheus.Unregister(m.SnapshotTotalSizeCounter)
	prometheus.Unregister(m.SnapshotLoadedSizeCounter)
	prometheus.Unregister(m.IncrementPeddingSizeGauge)
	prometheus.Unregister(m.IncrementLoadedCounter)
	prometheus.Unregister(m.TableVersionsCounter)
	prometheus.Unregister(m.ErrorCounter)
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
