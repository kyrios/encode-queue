# The worker on the one toolchain (toolchain/): ffmpeg with libx265, libx264, libvmaf,
# libmp3lame and libass, and whisper-cli, at toolchain/versions, the same build video-pipeline's
# image uses, and the aligner's engine (v0.2.16). The models are not in the image: mount them at
# EQ_WHISPER_MODELS_DIR, the aligner's folder among them. The image holds no address, port or
# secret.
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

# The align job's engine for the CPU (v0.2.16, internal/aligner): python-build-standalone's
# CPython 3.12.12, relocatable, needing nothing of Debian's, and qwen-asr on PyTorch for the CPU,
# every package by its hashes, wheels only, none of qwen-asr's demo and server dependencies. The
# import at the end is the smoke test. The model is not in the image: the models folder holds it.
FROM ${TOOLCHAIN} AS aligner
ADD --checksum=sha256:6249bea270cdd4218a258edb3a1c5602dd9dc0f785b9d29963676276a3838e60 \
    https://github.com/astral-sh/python-build-standalone/releases/download/20260114/cpython-3.12.12%2B20260114-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz \
    /tmp/python.tar.gz
COPY internal/aligner/requirements-torch-cpu.txt internal/aligner/requirements-torch.txt /tmp/
RUN mkdir -p /opt/aligner && tar -xzf /tmp/python.tar.gz -C /opt/aligner \
 && P="/opt/aligner/python/bin/python3 -I -m pip install --no-deps --require-hashes --only-binary=:all: --no-cache-dir --no-input --disable-pip-version-check" \
 && $P --index-url https://download.pytorch.org/whl/cpu -r /tmp/requirements-torch-cpu.txt \
 && $P --index-url https://pypi.org/simple -r /tmp/requirements-torch.txt \
 && /opt/aligner/python/bin/python3 -I -c "from qwen_asr import Qwen3ForcedAligner; import torch; print(torch.__version__)"

FROM ${TOOLCHAIN}
RUN useradd --system --uid 10001 --home-dir /var/lib/encode-worker --create-home worker && mkdir -p /models
COPY --from=aligner /opt/aligner /opt/aligner
COPY --from=build /out/encode-worker /usr/local/bin/encode-worker
ENV EQ_WORKER_STATE_DIR=/var/lib/encode-worker \
    EQ_FFMPEG=/opt/encode-toolchain/bin/ffmpeg \
    EQ_FFPROBE=/opt/encode-toolchain/bin/ffprobe \
    EQ_WHISPER_CLI=/opt/encode-toolchain/bin/whisper-cli \
    EQ_WHISPER_MODELS_DIR=/models \
    EQ_ALIGNER_PYTHON=/opt/aligner/python/bin/python3
USER worker
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s CMD ["/usr/local/bin/encode-worker", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/encode-worker"]
CMD ["run"]
