package httpserver

import (
    "net/http"

    "github.com/diegodesousas/go-devkit/pkg/gen"
    "github.com/diegodesousas/go-devkit/pkg/log"
    "github.com/go-chi/chi/v5/middleware"
    "github.com/go-chi/cors"
)

func Logger(logger log.Logger) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            ctx := r.Context()
            ctx = log.WithLogger(ctx, logger)
            r = r.WithContext(ctx)
            next.ServeHTTP(w, r)
        })
    }
}

func RequestID(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        id := r.Header.Get("X-Request-ID")
        if id == "" {
            next.ServeHTTP(w, r)
            return
        }
        ctx := log.WithFields(r.Context(), log.NewField("request-id", id))
        r = r.WithContext(ctx)
        next.ServeHTTP(w, r)
    })
}

func TraceID(idGenerator gen.StringGenerator) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            id := idGenerator()
            ctx := log.WithFields(r.Context(), log.NewField("trace-id", id))
            r = r.WithContext(ctx)
            next.ServeHTTP(w, r)
        })
    }
}

func ContentTypeJson() Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            w.Header().Set("Content-Type", "application/json")
            next.ServeHTTP(w, r)
        })
    }
}

func Compress() Middleware {
    return middleware.Compress(5)
}

func AllowAll() Middleware {
    return cors.AllowAll().Handler
}
