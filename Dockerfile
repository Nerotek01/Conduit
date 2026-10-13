FROM golang:1.25-alpine AS gobuild
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -ldflags "-s -w" -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -ldflags "-s -w" -o /out/coordinator ./cmd/coordinator \
 && CGO_ENABLED=0 go build -ldflags "-s -w" -o /out/cli ./cmd/cli

FROM node:24-alpine AS dashbuild
WORKDIR /dash
COPY dashboard/package.json dashboard/package-lock.json* ./
RUN npm ci --no-fund --no-audit || npm install --no-fund --no-audit
COPY dashboard/ .
RUN npm run build

FROM alpine:3.20
RUN adduser -D -u 10001 conduit && apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=gobuild /out/ /app/bin/
COPY --from=dashbuild /dash/dist/ /app/dashboard/
USER conduit
ENV CONDUIT_HTTP_ADDR=:8080 \
    CONDUIT_GRPC_ADDR=:9090 \
    CONDUIT_PG_DSN=postgres://conduit:conduit@postgres:5432/conduit?sslmode=disable \
    CONDUIT_REDIS_ADDR=redis:6379 \
    CONDUIT_DASHBOARD_DIR=/app/dashboard
EXPOSE 8080 9090
ENTRYPOINT ["/app/bin/server"]
