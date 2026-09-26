package shutdown

import (
	"context"
	stderrors "errors"
	"time"
)

// Step releases one resource, such as the function returned by
// httpserver.Server.Run or a database connection's Close.
type Step func(ctx context.Context) error

// Graceful runs steps in order, all sharing a single deadline of timeout, so a
// step that never finishes cannot hang the process. Every step runs even if an
// earlier one fails or times out, so later resources are still released. The
// errors of the failing steps are joined; nil is returned when all succeed.
func Graceful(ctx context.Context, timeout time.Duration, steps ...Step) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var errs []error
	for _, step := range steps {
		if err := step(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return stderrors.Join(errs...)
}
