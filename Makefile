all: build  

build:
	go install ./...

clean:
	go clean -i ./...

test:
	go test ./...
	go test -race ./...