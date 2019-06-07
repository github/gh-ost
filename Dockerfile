FROM golang:1.12.5-alpine3.9 AS build_base
RUN apk add --no-cache curl git gcc g++
WORKDIR /go/src/app
COPY go.mod .
COPY go.sum .
ENV GO111MODULE=on
ENV GOOS=linux
ENV GOARCH=amd64
RUN go mod download

FROM build_base as builder
COPY . .
RUN go build ./...
RUN go install -v ./...

FROM alpine:3.9
RUN apk --no-cache add ca-certificates
COPY --from=builder /go/bin/gh-ost /gh-ost
