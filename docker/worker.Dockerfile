# The worker on the one toolchain (toolchain/): ffmpeg with libx265, libx264, libvmaf,
# libmp3lame and libass, and whisper-cli, at toolchain/versions, the same build video-pipeline's
# image uses. The models are not in the image: mount them at EQ_WHISPER_MODELS_DIR. The image
# holds no address, port or secret.
# Toolchain 3: libass, its shaping and fontconfig built from source on both platforms, Inter and
# a fonts.conf that sees only it (FONTCONFIG_FILE, set by the toolchain image), so burnt-in
# captions render the same on a Mac; each component's licence in /opt/encode-toolchain/licenses
# (ffmpeg here is GPLv3; its source is on the toolchain-3 release).
ARG TOOLCHAIN=ghcr.io/kyrios/encode-toolchain:3@sha256:90d801a26a7e695c3b64434bac97f4e95d3d0df664f25c264ae7dc3dc83f502e

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
