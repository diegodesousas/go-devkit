package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"

    "github.com/diegodesousas/go-devkit/pkg/gen"
    "github.com/diegodesousas/go-devkit/pkg/httpserver"
    pkgLog "github.com/diegodesousas/go-devkit/pkg/log"
)

var helloRoute = httpserver.NewGet(
    "/hello/{name}",
    func(w http.ResponseWriter, req *http.Request) error {
        logger := pkgLog.FromContext(req.Context())
        logger.Info("teste")
        nameParam := httpserver.GetParam(req, "name")

        w.Header().Set("Content-Type", "text/plain")

        _, err := w.Write([]byte("Hello " + nameParam))
        if err != nil {
            return err
        }

        return nil
    },
)

func main() {

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(helloRoute),
        httpserver.WithMiddlewares(
            httpserver.RequestID,
            httpserver.TraceID(gen.UUIDGenerator()),
            httpserver.Compress(),
        ),
    )

    log.Println("Server starting...")
    shutdown := server.Run()

    interrupt := make(chan os.Signal, 1)
    signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        if err := <-server.ShutdownListener(); err != nil && err != http.ErrServerClosed {
            interrupt <- syscall.SIGTERM
        }
    }()

    log.Println("Server running on port 8080")
    <-interrupt

    if err := shutdown(context.Background()); err != nil {
        log.Fatal(err)
    }

    log.Println("Server shutdown completed")
}
