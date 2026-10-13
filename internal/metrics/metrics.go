package metrics

import (
        "net/http"
        "time"

        "github.com/prometheus/client_golang/prometheus"
        "github.com/prometheus/client_golang/prometheus/promauto"
        "github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
        registry       *prometheus.Registry
        JobsSubmitted  prometheus.Counter
        JobsSucceeded  prometheus.Counter
        JobsRetried    prometheus.Counter
        JobsDead       prometheus.Counter
        JobsCancelled  prometheus.Counter
        JobDuration    prometheus.Histogram
        SubmitLatency  prometheus.Histogram
        QueueDepth     *prometheus.GaugeVec
        ActiveWorkers  prometheus.Gauge
        InFlightJobs   prometheus.Gauge
        WSclients      prometheus.Gauge
}

func New() *Metrics {
        reg := prometheus.NewRegistry()
        factory := promauto.With(reg)
        m := &Metrics{
                registry: reg,
                JobsSubmitted: factory.NewCounter(prometheus.CounterOpts{
                        Namespace: "conduit",
                        Name:      "jobs_submitted_total",
                }),
                JobsSucceeded: factory.NewCounter(prometheus.CounterOpts{
                        Namespace: "conduit",
                        Name:      "jobs_succeeded_total",
                }),
                JobsRetried: factory.NewCounter(prometheus.CounterOpts{
                        Namespace: "conduit",
                        Name:      "jobs_retried_total",
                }),
                JobsDead: factory.NewCounter(prometheus.CounterOpts{
                        Namespace: "conduit",
                        Name:      "jobs_dead_total",
                }),
                JobsCancelled: factory.NewCounter(prometheus.CounterOpts{
                        Namespace: "conduit",
                        Name:      "jobs_cancelled_total",
                }),
                JobDuration: factory.NewHistogram(prometheus.HistogramOpts{
                        Namespace: "conduit",
                        Name:      "job_duration_seconds",
                        Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
                }),
                SubmitLatency: factory.NewHistogram(prometheus.HistogramOpts{
                        Namespace: "conduit",
                        Name:      "submit_latency_seconds",
                        Buckets:   []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
                }),
                QueueDepth: factory.NewGaugeVec(prometheus.GaugeOpts{
                        Namespace: "conduit",
                        Name:      "queue_depth",
                }, []string{"stream"}),
                ActiveWorkers: factory.NewGauge(prometheus.GaugeOpts{
                        Namespace: "conduit",
                        Name:      "active_workers",
                }),
                InFlightJobs: factory.NewGauge(prometheus.GaugeOpts{
                        Namespace: "conduit",
                        Name:      "in_flight_jobs",
                }),
                WSclients: factory.NewGauge(prometheus.GaugeOpts{
                        Namespace: "conduit",
                        Name:      "websocket_clients",
                }),
        }
        return m
}

func (m *Metrics) Registry() *prometheus.Registry {
        return m.registry
}

func (m *Metrics) Handler() http.Handler {
        return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
                Timeout: 5 * time.Second,
        })
}

func (m *Metrics) ObserveSubmission(start time.Time) {
        m.SubmitLatency.Observe(time.Since(start).Seconds())
}

func (m *Metrics) RecordQueueDepth(depths map[string]int64) {
        for name, depth := range depths {
                m.QueueDepth.WithLabelValues(name).Set(float64(depth))
        }
}
