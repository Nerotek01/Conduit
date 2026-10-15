package api

import (
        "bufio"
        "context"
        "crypto/rand"
        "encoding/hex"
        "errors"
        "io"
        "log/slog"
        "net"
        "net/http"
        "time"
)

type ctxKey int

const requestIDKey ctxKey = 1

type responseWriter struct {
        http.ResponseWriter
        status int
}

func (w *responseWriter) WriteHeader(code int) {
        w.status = code
        w.ResponseWriter.WriteHeader(code)
}

// Hijack forwards the optional http.Hijacker interface so middleware wrapping
// does not break WebSocket upgrades. Without this, gorilla's Upgrade fails
// with HTTP 500 because the wrapper hides the underlying connection.
func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
        h, ok := w.ResponseWriter.(http.Hijacker)
        if !ok {
                return nil, nil, errors.New("responsewriter does not implement http.Hijacker")
        }
        return h.Hijack()
}

// Flush forwards the optional http.Flusher interface for streaming responses.
func (w *responseWriter) Flush() {
        if f, ok := w.ResponseWriter.(http.Flusher); ok {
                f.Flush()
        }
}

// Unwrap gives http.ResponseController access to the underlying writer.
func (w *responseWriter) Unwrap() http.ResponseWriter {
        return w.ResponseWriter
}

func withRequestID(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                buf := make([]byte, 8)
                if _, err := rand.Read(buf); err != nil {
                        http.Error(w, "internal error", http.StatusInternalServerError)
                        return
                }
                id := hex.EncodeToString(buf)
                w.Header().Set("X-Request-Id", id)
                ctx := context.WithValue(r.Context(), requestIDKey, id)
                next.ServeHTTP(&responseWriter{ResponseWriter: w, status: http.StatusOK}, r.WithContext(ctx))
        })
}

func withLogging(logger *slog.Logger, next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                start := time.Now()
                next.ServeHTTP(w, r)
                rw, ok := w.(*responseWriter)
                status := http.StatusOK
                if ok {
                        status = rw.status
                }
                logger.Info("http request",
                        "method", r.Method,
                        "path", r.URL.Path,
                        "status", status,
                        "duration_ms", time.Since(start).Milliseconds(),
                        "request_id", RequestID(r.Context()),
                        "remote", r.RemoteAddr,
                )
        })
}

func withRecover(logger *slog.Logger, next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                defer func() {
                        if rec := recover(); rec != nil {
                                logger.Error("handler panic", "path", r.URL.Path, "panic", rec)
                                w.Header().Set("Content-Type", "application/json")
                                w.WriteHeader(http.StatusInternalServerError)
                                _, _ = io.WriteString(w, `{"error":"internal server error"}`)
                        }
                }()
                next.ServeHTTP(w, r)
        })
}

func withTimeouts(next http.Handler) http.Handler {
        return http.TimeoutHandler(next, 30*time.Second, `{"error":"request timeout"}`)
}

func RequestID(ctx context.Context) string {
        if v, ok := ctx.Value(requestIDKey).(string); ok {
                return v
        }
        return ""
}
