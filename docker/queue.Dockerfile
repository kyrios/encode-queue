# The queue server. The image holds no address, port, subnet or secret: all of them come from
# the environment (README, "Configuration").
FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/encode-queue ./cmd/encode-queue

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/encode-queue /usr/local/bin/encode-queue
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/encode-queue"]
