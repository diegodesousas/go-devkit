package consumer_test

import (
	"os"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
)

// TestMain activates the mock tracer for the package. Until a tracer is
// started, tracer.StartSpan returns a span whose context is not valid, and
// tracer.Inject is a silent no-op, which would make trace propagation
// untestable. See pkg/stream/dispatcher/main_test.go for the same setup.
func TestMain(m *testing.M) {
	mt := mocktracer.Start()

	code := m.Run()

	mt.Stop()

	os.Exit(code)
}
