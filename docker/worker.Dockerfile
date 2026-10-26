# The worker on the one toolchain (toolchain/): ffmpeg with libx265, libx264, libvmaf,
# libmp3lame and libass, and whisper-cli, at toolchain/versions, the same build video-pipeline's
# image uses. The models are not in the image: mount them at EQ_WHISPER_MODELS_DIR. The image
# holds no address, port or secret.
ARG TOOLCHAIN=ghcr.io/kyrios/encode-toolchain:1

FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/encode-worker ./cmd/encode-worker

FROM ${TOOLCHAIN}
RUN useradd --system --uid 10001 --home-dir /var/lib/encode-worker --create-home worker && mkdir -p /models
COPY --from=build /out/encode-worker /usr/local/bin/encode-worker
ENV EQ_WORKER_STATE_DIR=/var/lib/encode-worker \
    EQ_FFMPEG=/opt/encode-toolchain/bin/ffmpeg \
    EQ_FFPROBE=/opt/encode-toolchain/bin/ffprobe \
    EQ_WHISPER_CLI=/opt/encode-toolchain/bin/whisper-cli \
    EQ_WHISPER_MODELS_DIR=/models
USER worker
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s CMD ["/usr/local/bin/encode-worker", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/encode-worker"]
CMD ["run"]
