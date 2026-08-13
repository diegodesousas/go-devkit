package metrics

import (
	"context"
	"net/http"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/diegodesousas/go-devkit/pkg/log"
)

// metricKey is the context key holding the Metric client. An unexported struct
// type cannot collide with a key set by another package, which a plain string
// can.
type metricKey struct{}

// defaultRate is the statsd sample rate used when a caller does not pick one.
// It must be 1: the rate is a sampling probability, so zero would mean "never
// emit this metric".
const defaultRate = 1.0

type Metric interface {
	statsd.ClientInterface
}

func New() (Metric, error) {
	return statsd.New("localhost:8125")
}

func putInContext(parent context.Context, metric statsd.ClientInterface) context.Context {
	return context.WithValue(parent, metricKey{}, metric)
}

func fromContext(ctx context.Context) Metric {
	metric, ok := ctx.Value(metricKey{}).(Metric)
	if !ok {
		return nil
	}

	return metric
}

func Increment(ctx context.Context, name string, tags ...string) {
	if metric := fromContext(ctx); metric != nil {
		err := metric.Incr(name, tags, defaultRate)
		if err != nil {
			log.FromContext(ctx).Error(err.Error())
		}
	}
}

func Histogram(ctx context.Context, name string, value float64, rate float64, tags ...string) {
	if metric := fromContext(ctx); metric != nil {
		err := metric.Histogram(name, value, tags, rate)
		if err != nil {
			log.FromContext(ctx).Error(err.Error())
		}
	}
}

func Gauge(ctx context.Context, name string, value float64, rate float64, tags ...string) {
	if metric := fromContext(ctx); metric != nil {
		err := metric.Gauge(name, value, tags, rate)
		if err != nil {
			log.FromContext(ctx).Error(err.Error())
		}
	}
}

func Metrics(metric Metric) func(handler http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := putInContext(r.Context(), metric)
			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}
