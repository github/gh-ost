FROM golang:1.17 AS build
LABEL maintainer="github@github.com"

COPY . /go/src/github.com/github/gh-ost
WORKDIR /go/src/github.com/github/gh-ost

RUN go build -o gh-ost go/cmd/gh-ost/main.go
RUN go build -o gh-ost-localtests go/cmd/gh-ost-localtests/main.go



FROM debian:buster-slim AS image

RUN apt-get update
RUN apt-get install -y default-mysql-client
RUN rm -rf /var/lib/apt/lists/*

COPY --from=build /go/src/github.com/github/gh-ost/gh-ost /usr/local/bin/gh-ost
COPY --from=build /go/src/github.com/github/gh-ost/gh-ost-localtests /usr/local/bin/gh-ost-localtests
COPY --from=build /go/src/github.com/github/gh-ost/localtests /etc/localtests

ENTRYPOINT ["gh-ost-localtests"]
