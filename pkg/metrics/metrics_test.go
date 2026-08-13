package metrics

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/diegodesousas/go-devkit/pkg/log"
	"github.com/stretchr/testify/assert"
)

// clientMock records what reaches the statsd client. It embeds NoOpClient so
// only the methods under test need an implementation - Metric embeds the whole
// statsd.ClientInterface, which is far too wide to write out by hand.
type clientMock struct {
	*statsd.NoOpClient
	name  string
	tags  []string
	rate  float64
	value float64
	err   error
}

func (c *clientMock) Incr(name string, tags []string, rate float64) error {
	c.name = name
	c.tags = tags
	c.rate = rate

	return c.err
}

func (c *clientMock) Histogram(name string, value float64, tags []string, rate float64) error {
	c.name = name
	c.value = value
	c.tags = tags
	c.rate = rate

	return c.err
}

func (c *clientMock) Gauge(name string, value float64, tags []string, rate float64) error {
	c.name = name
	c.value = value
	c.tags = tags
	c.rate = rate

	return c.err
}

func TestIncrement_UsesRateOne(t *testing.T) {
	var (
		expectedName = "go-devkit.test.counter"
		expectedTags = []string{"status:200"}
		expectedRate = 1.0
	)

	metricMock := &clientMock{NoOpClient: &statsd.NoOpClient{}}

	ctx := putInContext(context.Background(), metricMock)
	Increment(ctx, expectedName, expectedTags...)

	assert.Equal(t, expectedName, metricMock.name)
	assert.Equal(t, expectedTags, metricMock.tags)

	// The rate is a sampling probability. Zero means "never emit", so anything
	// below 1 silently drops counters.
	assert.Equal(t, expectedRate, metricMock.rate)
}

func TestIncrement_LogsTheClientError(t *testing.T) {
	var expectedErr = errors.New("statsd is unreachable")

	metricMock := &clientMock{NoOpClient: &statsd.NoOpClient{}, err: expectedErr}

	output := &bytes.Buffer{}
	ctx := log.WithLogger(context.Background(), log.New(log.WithOutput(output)))
	ctx = putInContext(ctx, metricMock)

	Increment(ctx, "go-devkit.test.counter")

	assert.Contains(t, output.String(), expectedErr.Error())
}

func TestHistogram(t *testing.T) {
	var (
		expectedName  = "go-devkit.test.histogram"
		expectedValue = 42.0
		expectedRate  = 0.5
		expectedTags  = []string{"method:GET"}
	)

	metricMock := &clientMock{NoOpClient: &statsd.NoOpClient{}}

	ctx := putInContext(context.Background(), metricMock)
	Histogram(ctx, expectedName, expectedValue, expectedRate, expectedTags...)

	assert.Equal(t, expectedName, metricMock.name)
	assert.Equal(t, expectedValue, metricMock.value)
	assert.Equal(t, expectedRate, metricMock.rate)
	assert.Equal(t, expectedTags, metricMock.tags)
}

func TestGauge(t *testing.T) {
	var (
		expectedName  = "go-devkit.test.gauge"
		expectedValue = 7.0
		expectedRate  = 1.0
		expectedTags  = []string{"queue:orders"}
	)

	metricMock := &clientMock{NoOpClient: &statsd.NoOpClient{}}

	ctx := putInContext(context.Background(), metricMock)
	Gauge(ctx, expectedName, expectedValue, expectedRate, expectedTags...)

	assert.Equal(t, expectedName, metricMock.name)
	assert.Equal(t, expectedValue, metricMock.value)
	assert.Equal(t, expectedRate, metricMock.rate)
	assert.Equal(t, expectedTags, metricMock.tags)
}

func TestIncrement_WithoutMetricInContextIsNoOp(t *testing.T) {
	assert.NotPanics(t, func() {
		Increment(context.Background(), "go-devkit.test.counter", "status:200")
	})
}

func TestFromContext_ReturnsNilWhenAbsent(t *testing.T) {
	assert.Nil(t, fromContext(context.Background()))
}

func TestFromContext_ReturnsTheStoredMetric(t *testing.T) {
	expectedMetric := &clientMock{NoOpClient: &statsd.NoOpClient{}}

	ctx := putInContext(context.Background(), expectedMetric)

	assert.Equal(t, expectedMetric, fromContext(ctx))
}

func TestFromContext_IgnoresAPlainStringKey(t *testing.T) {
	expectedMetric := &clientMock{NoOpClient: &statsd.NoOpClient{}}

	// The key used to be the plain string "metric", so any package storing that
	// same string could be picked up here by accident.
	ctx := context.WithValue(context.Background(), "metric", expectedMetric) //nolint:staticcheck

	assert.Nil(t, fromContext(ctx))
}

func TestMetrics_PutsTheMetricInTheRequestContext(t *testing.T) {
	expectedMetric := &clientMock{NoOpClient: &statsd.NoOpClient{}}

	var got Metric
	handler := Metrics(expectedMetric)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = fromContext(r.Context())
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, expectedMetric, got)
}
