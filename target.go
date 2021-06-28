package sql_exporter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/free/sql_exporter/config"
	"github.com/free/sql_exporter/errors"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	// Capacity for the channel to collect metrics.
	capMetricChan = 1000

	upMetricName       = "up"
	upMetricHelp       = "1 if the target is reachable, or 0 if the scrape failed"
	scrapeDurationName = "scrape_duration_seconds"
	scrapeDurationHelp = "How long it took to scrape the target in seconds"
)

// Target collects SQL metrics from a single sql.DB instance. It aggregates one or more Collectors and it looks much
// like a prometheus.Collector, except its Collect() method takes a Context to run in.
type Target interface {
	// Collect is the equivalent of prometheus.Collector.Collect(), but takes a context to run in.
	Collect(ctx context.Context, ch chan<- Metric)
	Close()	error
}

// target implements Target. It wraps a sql.DB, which is initially nil but never changes once instantianted.
type TargetStruct struct {
	Name               string
	Dsn                string
	Collectors         []Collector
	ConstLabels        prometheus.Labels
	GlobalConfig       *config.GlobalConfig
	UpDesc             MetricDesc
	ScrapeDurationDesc MetricDesc
	LogContext         string

	Conn *sql.DB
}

func (t *TargetStruct) Close() error {
	err := t.Conn.Close()
	if err != nil {
		return err
	}
	return nil
}


// NewTarget returns a new Target with the given instance name, data source name, collectors and constant labels.
// An empty target name means the exporter is running in single target mode: no synthetic metrics will be exported.
func NewTarget(
	logContext, name, dsn string, ccs []*config.CollectorConfig, constLabels prometheus.Labels, gc *config.GlobalConfig) (
	Target, errors.WithContext) {

	if name != "" {
		logContext = fmt.Sprintf("%s, target=%q", logContext, name)
	}

	constLabelPairs := make([]*dto.LabelPair, 0, len(constLabels))
	for n, v := range constLabels {
		constLabelPairs = append(constLabelPairs, &dto.LabelPair{
			Name:  proto.String(n),
			Value: proto.String(v),
		})
	}
	sort.Sort(labelPairSorter(constLabelPairs))

	collectors := make([]Collector, 0, len(ccs))
	for _, cc := range ccs {
		c, err := NewCollector(logContext, cc, constLabelPairs)
		if err != nil {
			return nil, err
		}
		collectors = append(collectors, c)
	}

	upDesc := NewAutomaticMetricDesc(logContext, upMetricName, upMetricHelp, prometheus.GaugeValue, constLabelPairs)
	scrapeDurationDesc :=
		NewAutomaticMetricDesc(logContext, scrapeDurationName, scrapeDurationHelp, prometheus.GaugeValue, constLabelPairs)
	t := TargetStruct{
		Name:               name,
		Dsn:                dsn,
		Collectors:         collectors,
		ConstLabels:        constLabels,
		GlobalConfig:       gc,
		UpDesc:             upDesc,
		ScrapeDurationDesc: scrapeDurationDesc,
		LogContext:         logContext,
	}
	return &t, nil
}

// Collect implements Target.
func (t *TargetStruct) Collect(ctx context.Context, ch chan<- Metric) {
	var (
		scrapeStart = time.Now()
		targetUp    = true
	)

	err := t.ping(ctx)
	if err != nil {
		ch <- NewInvalidMetric(errors.Wrap(t.LogContext, err))
		targetUp = false
	}
	if t.Name != "" {
		// Export the target's `up` metric as early as we know what it should be.
		ch <- NewMetric(t.UpDesc, boolToFloat64(targetUp))
	}

	var wg sync.WaitGroup
	// Don't bother with the collectors if target is down.
	if targetUp {
		wg.Add(len(t.Collectors))
		for _, c := range t.Collectors {
			// If using a single DB connection, collectors will likely run sequentially anyway. But we might have more.
			go func(collector Collector) {
				defer wg.Done()
				collector.Collect(ctx, t.Conn, ch)
			}(c)
		}
	}
	// Wait for all collectors (if any) to complete.
	wg.Wait()

	if t.Name != "" {
		// And export a `scrape duration` metric once we're done scraping.
		ch <- NewMetric(t.ScrapeDurationDesc, float64(time.Since(scrapeStart))*1e-9)
	}
}

func (t *TargetStruct) ping(ctx context.Context) errors.WithContext {
	// Create the DB handle, if necessary. It won't usually open an actual connection, so we'll need to ping afterwards.
	// We cannot do this only once at creation time because the sql.Open() documentation says it "may" open an actual
	// connection, so it "may" actually fail to open a handle to a DB that's initially down.
	if t.Conn == nil {
		conn, err := OpenConnection(ctx, t.LogContext, t.Dsn, t.GlobalConfig.MaxConns, t.GlobalConfig.MaxIdleConns)
		if err != nil {
			if err != ctx.Err() {
				return errors.Wrap(t.LogContext, err)
			}
			// if err == ctx.Err() fall through
		} else {
			t.Conn = conn
		}
	}

	// If we have a handle and the context is not closed, test whether the database is up.
	if t.Conn != nil && ctx.Err() == nil {
		var err error
		// Ping up to max_connections + 1 times as long as the returned error is driver.ErrBadConn, to purge the connection
		// pool of bad connections. This might happen if the previous scrape timed out and in-flight queries got canceled.
		for i := 0; i <= t.GlobalConfig.MaxConns; i++ {
			if err = PingDB(ctx, t.Conn); err != driver.ErrBadConn {
				break
			}
		}
		if err != nil {
			return errors.Wrap(t.LogContext, err)
		}
	}

	if ctx.Err() != nil {
		return errors.Wrap(t.LogContext, ctx.Err())
	}
	return nil
}

// boolToFloat64 converts a boolean flag to a float64 value (0.0 or 1.0).
func boolToFloat64(value bool) float64 {
	if value {
		return 1.0
	}
	return 0.0
}
