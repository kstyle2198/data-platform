FROM golang:1.24-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git ca-certificates

RUN git clone https://github.com/minio/minio.git .

RUN go build -o /minio .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates

COPY --from=builder /minio /usr/local/bin/minio

RUN mkdir -p /data

EXPOSE 9000
EXPOSE 9001

VOLUME ["/data"]

ENTRYPOINT ["minio"]

CMD ["server", "/data", "--console-address", ":9001"]
