package version

// Version is the fallback shown by plain `go build`; `make build` and the
// release pipeline override it via -ldflags -X.
const Version = "0.1.3"
