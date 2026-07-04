# Build stage: static binaries so the runtime image needs no libc.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/basalt-server ./cmd/basalt-server \
 && CGO_ENABLED=0 go build -trimpath -o /out/basalt ./cmd/basalt

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/basalt-server /basalt-server
COPY --from=build /out/basalt /basalt
VOLUME /data
EXPOSE 7654 7655
ENTRYPOINT ["/basalt-server"]
CMD ["-data-dir", "/data", "-listen", "0.0.0.0:7654", "-metrics-listen", "0.0.0.0:7655"]
