# The toolchain for linux/amd64: ffmpeg, ffprobe, fc-list and whisper-cli of toolchain/versions in
# /opt/encode-toolchain/bin, Inter and its fonts.conf in /opt/encode-toolchain/fonts, on Debian
# 13 with the few runtime libraries they link (zlib, OpenMP). The caption stack (libass,
# harfbuzz, fribidi, freetype, fontconfig) is built from source, as on macOS, so both render the
# same captions. The worker image and video-pipeline's image build FROM this. git is there only
# because x265 reads its version from x265Version.txt only when git is found.
FROM debian:trixie-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates cmake curl git \
      gperf nasm ninja-build pkg-config python3-venv unzip xxd zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
COPY versions build.sh /toolchain/
# meson as toolchain/versions pins it (Debian's is too old for fontconfig), only for the build.
RUN . /toolchain/versions && python3 -m venv /opt/meson \
    && echo "meson==$MESON_VERSION --hash=sha256:$MESON_SHA256" > /tmp/meson.txt \
    && /opt/meson/bin/pip install --no-cache-dir --only-binary :all: --require-hashes -r /tmp/meson.txt \
    && ln -s /opt/meson/bin/meson /usr/local/bin/meson
COPY check /toolchain/check/
RUN /toolchain/build.sh /opt/encode-toolchain /tmp/toolchain && rm -rf /tmp/toolchain

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libgomp1 zlib1g \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/encode-toolchain /opt/encode-toolchain
RUN mkdir -p /usr/local/share/encode-toolchain \
    && ln -s /opt/encode-toolchain/bin/versions.json /opt/encode-toolchain/bin/versions.txt /usr/local/share/encode-toolchain/
# Every process of this image and those built FROM it finds the toolchain's fonts and no others.
ENV PATH=/opt/encode-toolchain/bin:$PATH \
    FONTCONFIG_FILE=/opt/encode-toolchain/fonts/fonts.conf
RUN ffmpeg -hide_banner -version | head -1 && ffmpeg -hide_banner -filters | grep -q ' ass ' \
    && fc-list : family | grep -q '^Inter' \
    && whisper-cli --help >/dev/null 2>&1 && cat /opt/encode-toolchain/bin/versions.txt \
    && test -s /opt/encode-toolchain/licenses/SOURCES.md && ls /opt/encode-toolchain/licenses
