FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/node

FROM alpine:3.20
RUN adduser -D -H boilerpulse
WORKDIR /app
COPY --from=build /out/node /usr/local/bin/node
COPY configs ./configs
USER boilerpulse
EXPOSE 8080
EXPOSE 9080
ENTRYPOINT ["node"]
