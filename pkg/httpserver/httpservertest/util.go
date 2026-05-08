package httpservertest

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func SetURLParam(r *http.Request, key, value string) {
	chiCtx := chi.RouteContext(r.Context())
	if chiCtx == nil {
		chiCtx = chi.NewRouteContext()
	}

	chiCtx.URLParams.Add(key, value)

	rCtx := context.WithValue(r.Context(), chi.RouteCtxKey, chiCtx)

	*r = *r.WithContext(rCtx)
}
