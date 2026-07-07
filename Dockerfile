FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/s3store .

FROM alpine:3.20
RUN adduser -D -H -u 10001 s3store
USER s3store
WORKDIR /app
COPY --from=build /out/s3store /app/s3store
EXPOSE 9000 9001
ENTRYPOINT ["/app/s3store"]
