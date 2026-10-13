package api

import (
        "context"
        "errors"
        "log/slog"
        "net/http"
        "os"
        "path/filepath"
        "time"

        "github.com/Nerotek01/Conduit/internal/events"
)

type ServerOptions struct {
        HTTPAddr     string
        DashboardDir string
        MaxBodyBytes int64
        WSMaxClients int
        Logger       *slog.Logger
        TraceWrap    func(http.Handler) http.Handler
}

type Server struct {
        httpServer *http.Server
        hub        *Hub
}

func NewServer(svc *Service, opts ServerOptions, met interface {
        Handler() http.Handler
}) *Server {
        mux := http.NewServeMux()

        // http.TimeoutHandler wraps the ResponseWriter in a type that does NOT
        // implement http.Hijacker, which breaks the WebSocket upgrade. Apply it
        // per-route to REST endpoints only; /v1/ws manages its own deadlines via
        // the hub's read/write pumps, and /metrics + /healthz must answer even
        // under load.
        timed := func(h http.HandlerFunc) http.Handler { return withTimeouts(h) }

        mux.Handle("POST /v1/jobs", timed(svc.handleSubmitJob))
        mux.Handle("GET /v1/jobs", timed(svc.handleListJobs))
        mux.Handle("GET /v1/jobs/{id}", timed(svc.handleGetJob))
        mux.Handle("DELETE /v1/jobs/{id}", timed(svc.handleCancelJob))
        mux.Handle("POST /v1/jobs/{id}/requeue", timed(svc.handleRequeueJob))
        mux.Handle("POST /v1/workflows", timed(svc.handleCreateWorkflow))
        mux.Handle("GET /v1/workflows", timed(svc.handleListWorkflows))
        mux.Handle("GET /v1/workflows/{id}", timed(svc.handleGetWorkflow))
        mux.Handle("POST /v1/workflows/{id}/run", timed(svc.handleRunWorkflow))
        mux.Handle("GET /v1/workflows/{id}/runs", timed(svc.handleListRuns))
        mux.Handle("GET /v1/queues", timed(svc.handleQueues))
        mux.Handle("GET /v1/workers", timed(svc.handleWorkers))
        mux.HandleFunc("GET /v1/metrics", met.Handler().ServeHTTP)
        mux.HandleFunc("GET /metrics", met.Handler().ServeHTTP)
        mux.HandleFunc("GET /healthz", svc.handleHealthz)
        mux.Handle("POST /v1/schedules", timed(svc.handleCreateSchedule))
        mux.Handle("GET /v1/schedules", timed(svc.handleListSchedules))
        mux.Handle("DELETE /v1/schedules/{id}", timed(svc.handleDeleteSchedule))

        hub := NewHub(opts.WSMaxClients, svc.met)
        mux.HandleFunc("GET /v1/ws", hub.HandleWS)

        if opts.DashboardDir != "" {
                fs := http.FileServer(neuteredFileSystem{http.Dir(opts.DashboardDir)})
                mux.Handle("GET /", fs)
        }

        // No global withTimeouts here: it wraps the ResponseWriter in a type
        // that does not implement http.Hijacker, which breaks the WebSocket
        // upgrade on /v1/ws. Timeouts are applied per REST route instead.
        handler := withRequestID(withLogging(opts.Logger, withRecover(opts.Logger, mux)))
        if opts.TraceWrap != nil {
                handler = opts.TraceWrap(handler)
        }

        return &Server{
                httpServer: &http.Server{
                        Addr:              opts.HTTPAddr,
                        Handler:           handler,
                        ReadHeaderTimeout: 5 * time.Second,
                        ReadTimeout:       15 * time.Second,
                        WriteTimeout:      30 * time.Second,
                        IdleTimeout:       75 * time.Second,
                        MaxHeaderBytes:    16 * 1024,
                },
                hub: hub,
        }
}

type neuteredFileSystem struct {
        fs http.FileSystem
}

func (n neuteredFileSystem) Open(path string) (http.File, error) {
        f, err := n.fs.Open(path)
        if err != nil {
                return nil, err
        }
        stat, err := f.Stat()
        if err != nil {
                return nil, err
        }
        if stat.IsDir() {
                index := filepath.Join(path, "index.html")
                if _, err := n.fs.Open(index); err != nil {
                        return nil, os.ErrNotExist
                }
        }
        return f, nil
}

func (s *Server) Start(ctx context.Context, bus events.Bus, snapshot func() ([]byte, bool)) error {
        go s.hub.Run(bus, snapshot)
        errCh := make(chan error, 1)
        go func() {
                if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
                        errCh <- err
                }
        }()
        select {
        case err := <-errCh:
                return err
        case <-ctx.Done():
                shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
                defer cancel()
                return s.httpServer.Shutdown(shutdownCtx)
        }
}

func (s *Server) Handler() http.Handler {
        return s.httpServer.Handler
}
