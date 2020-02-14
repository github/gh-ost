all: build

build:
	go build -o bin/go-mysqlbinlog cmd/go-mysqlbinlog/main.go 
	go build -o bin/go-mysqldump cmd/go-mysqldump/main.go 
	go build -o bin/go-canal cmd/go-canal/main.go 
	go build -o bin/go-binlogparser cmd/go-binlogparser/main.go 
	
test:
	go test --race -timeout 2m ./...
	
clean:
	go clean -i ./...
	@rm -rf ./bin