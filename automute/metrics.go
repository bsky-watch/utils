package automute

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bskywatchautomute",
		Subsystem: "cache",
		Name:      "requests_total",
		Help:      "Number of requests that checked the cache.",
	}, []string{"list", "cache_result", "outcome"})
	negativeCacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bskywatchautomute",
		Subsystem: "cache",
		Name:      "negative_cache_entries_total",
		Help:      "Number of entries in negative result cache.",
	}, []string{"list"})
)
