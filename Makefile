clean:
	go clean -testcache

build:
	go build -tags test -v ./...

test:
	go test -tags test -v ./...

lint:
	golangci-lint run --build-tags test ./...

all: clean build test