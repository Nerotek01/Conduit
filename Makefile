GO ?= go
BIN := bin
VERSION := 0.1.3
MODULE := github.com/Nerotek01/Conduit
LDFLAGS := -s -w -X $(MODULE)/internal/version.Version=$(VERSION)
DASH_DIST := dashboard/dist

.PHONY: build proto dashboard test race lint clean docker run

build: dashboard
        @mkdir -p $(BIN)
        $(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/server ./cmd/server
        $(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/worker ./cmd/worker
        $(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/coordinator ./cmd/coordinator
        $(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/cli ./cmd/cli
        @echo "binaries in $(BIN) (version $(VERSION))"

proto:
        python3 -m grpc_tools.protoc -I api/proto \
                --plugin=protoc-gen-go=$$(go env GOPATH)/bin/protoc-gen-go \
                --plugin=protoc-gen-go-grpc=$$(go env GOPATH)/bin/protoc-gen-go-grpc \
                --go_out=. --go_opt=module=$(MODULE) \
                --go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
                api/proto/conduit/v1/conduit.proto

dashboard:
        cd dashboard && npm install --no-fund --no-audit && npm run build

test:
        # Integration tests share one PostgreSQL test database and one Redis:
        # packages must run sequentially or schedulers steal each other's due jobs.
        $(GO) test -p 1 ./... -count=1

race:
        $(GO) test -p 1 -race ./... -count=1

lint:
        $(GO) vet ./...

clean:
        rm -rf $(BIN) $(DASH_DIST)

docker:
        docker compose up --build

run:
        docker compose up
