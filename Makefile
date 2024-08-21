clean:
	go clean -testcache

build:
	go build -v ./...

test:
	go test -tags test -v ./...

all: clean build test