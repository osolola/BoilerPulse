FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -o /out/gateway ./cmd/gateway

FROM alpine:3.20
RUN adduser -D -H boilerpulse
WORKDIR /app
COPY --from=build /out/gateway /usr/local/bin/gateway
COPY configs ./configs
USER boilerpulse
EXPOSE 8090
ENTRYPOINT ["gateway"]
