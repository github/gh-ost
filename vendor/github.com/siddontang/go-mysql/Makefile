all: build

build:
	rm -rf vendor && ln -s _vendor/vendor vendor
	go build -o bin/go-mysqlbinlog cmd/go-mysqlbinlog/main.go 
	go build -o bin/go-mysqldump cmd/go-mysqldump/main.go 
	go build -o bin/go-canal cmd/go-canal/main.go 
	go build -o bin/go-binlogparser cmd/go-binlogparser/main.go 
	rm -rf vendor

test:
	rm -rf vendor && ln -s _vendor/vendor vendor
	go test --race -timeout 2m ./...
	rm -rf vendor
	
clean:
	go clean -i ./...
	@rm -rf ./bin

update_vendor:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	rm -r vendor && mv _vendor/vendor vendor || true
	rm -rf _vendor
ifdef PKG
	glide get --strip-vendor --skip-test ${PKG}
else
	glide update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests
	mkdir -p _vendor
	mv vendor _vendor/vendor
