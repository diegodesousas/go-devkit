// Package validator composes validation rules over a typed payload.
//
// A Rule is a function; a Validator is an ordered list of them:
//
//	v := validator.New(
//		func(ctx context.Context, o Order) error {
//			if validator.IsEmpty(o.ID) {
//				return validator.NewRequiredError("id")
//			}
//			return nil
//		},
//		func(ctx context.Context, o Order) error {
//			if o.Total < 0 {
//				return validator.NewInvalidError("total", o.Total)
//			}
//			return nil
//		},
//	)
//
//	if err := v.Validate(ctx, order); err != nil {
//		return err
//	}
//
// Rules run in the order they were given and Validate stops at the first
// failure, so it reports one problem per call rather than every problem in the
// payload.
//
// The errors carry a machine-readable ErrorCode alongside their message, which
// is what lets an HTTP error handler map a validation failure onto a status
// code without parsing text:
//
//	var vErr validator.Error
//	if errors.As(err, &vErr) && vErr.Code == validator.CodeNotFound {
//		w.WriteHeader(http.StatusNotFound)
//	}
//
// Validate wraps the rule error, so reach for errors.As rather than a type
// assertion.
package validator
