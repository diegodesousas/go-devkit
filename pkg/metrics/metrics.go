package metrics

import (
    "context"
    "net/http"

    "github.com/DataDog/datadog-go/v5/statsd"
    "github.com/diegodesousas/go-devkit/pkg/log"
)

var metricKeyCtx interface{} = "metric"

type Metric interface {
    statsd.ClientInterface
}

func New() (Metric, error) {
    return statsd.New("localhost:8125")
}

func putInContext(parent context.Context, metric statsd.ClientInterface) context.Context {
    return context.WithValue(parent, metricKeyCtx, metric)
}

func fromContext(ctx context.Context) Metric {
    metric, ok := ctx.Value(metricKeyCtx).(Metric)
    if !ok {
        return nil
    }

    return metric
}

func Increment(ctx context.Context, name string, tags ...string) {
    if metric := fromContext(ctx); metric != nil {
        err := metric.Incr(name, tags, 0)
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
