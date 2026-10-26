# The worker, with Debian 13's ffmpeg (libx265, libx264), fonts-inter for the burnt-in captions,
# and whisper.cpp built from the release `encoder` runs (infra roles/encoder/defaults:
# encoder_whisper_version, encoder_whisper_commit). The models are not in the image: mount them
# at EQ_WHISPER_MODELS_DIR. The image holds no address, port or secret.
FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/encode-worker ./cmd/encode-worker

FROM debian:trixie-slim AS whisper
ARG WHISPER_VERSION=v1.9.5
ARG WHISPER_COMMIT=d1be6fde11ac6e0407606b4e42fe72d34add8037
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates cmake g++ git make \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "${WHISPER_VERSION}" https://github.com/ggml-org/whisper.cpp.git /whisper \
    && test "$(git -C /whisper rev-parse HEAD)" = "${WHISPER_COMMIT}"
# encoder builds with GGML_NATIVE=ON for its own CPU; an image runs on any x86-64-v3 machine
# (AVX2, FMA, F16C), so it names those instead.
RUN set -e; arch="$(uname -m)"; extra=""; \
    if [ "$arch" = x86_64 ]; then extra="-DGGML_AVX=ON -DGGML_AVX2=ON -DGGML_FMA=ON -DGGML_F16C=ON -DGGML_BMI2=ON"; fi; \
    cmake -S /whisper -B /build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DGGML_NATIVE=OFF $extra \
      -DGGML_CCACHE=OFF -DWHISPER_BUILD_TESTS=OFF -DWHISPER_BUILD_SERVER=OFF -DWHISPER_SDL2=OFF -DWHISPER_CURL=OFF \
    && cmake --build /build --config Release --target whisper-cli -j "$(nproc)" \
    && install -m 0755 /build/bin/whisper-cli /usr/local/bin/whisper-cli

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg fonts-inter fontconfig libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/encode-worker --create-home worker \
    && mkdir -p /models
COPY --from=whisper /usr/local/bin/whisper-cli /usr/local/bin/whisper-cli
COPY --from=build /out/encode-worker /usr/local/bin/encode-worker
ARG WHISPER_VERSION=v1.9.5
ENV EQ_WHISPER_VERSION=${WHISPER_VERSION} \
    EQ_WORKER_STATE_DIR=/var/lib/encode-worker \
    EQ_WHISPER_MODELS_DIR=/models
USER worker
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s CMD ["/usr/local/bin/encode-worker", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/encode-worker"]
CMD ["run"]
