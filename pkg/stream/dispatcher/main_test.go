package dispatcher_test

import (
	"os"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
)

// TestMain activates the mock tracer for the package. Until a tracer is
// started, tracer.StartSpanFromContext returns a nil span and tracer.Inject is
// a silent no-op, which would make trace propagation untestable.
func TestMain(m *testing.M) {
	mt := mocktracer.Start()

	code := m.Run()

	mt.Stop()

	os.Exit(code)
}
