clean:
	go clean -testcache

build:
	go build -tags test -v ./...

test:
	go test -tags test -v ./...

all: clean build test