FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/p1-relay .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/p1-relay /p1-relay
USER nonroot:nonroot
EXPOSE 8888 9090
ENTRYPOINT ["/p1-relay"]
