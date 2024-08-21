clean:
	go clean -testcache

build:
	go build -v ./...

test:
	go test -v ./...

all: clean build test