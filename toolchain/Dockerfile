# The toolchain for linux/amd64: ffmpeg, ffprobe and whisper-cli of toolchain/versions in
# /opt/encode-toolchain/bin, on Debian 13 with the runtime libraries they link (libass and its
# fonts stack, OpenMP). The worker image and video-pipeline's image build FROM this. git is
# there only because x265 reads its version from x265Version.txt only when git is found.
FROM debian:trixie-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates cmake curl git \
      libass-dev meson nasm ninja-build pkg-config xxd zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
COPY versions build.sh /toolchain/
RUN /toolchain/build.sh /opt/encode-toolchain /tmp/toolchain && rm -rf /tmp/toolchain

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates fontconfig fonts-inter libass9 \
      libgomp1 zlib1g \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/encode-toolchain /opt/encode-toolchain
RUN mkdir -p /usr/local/share/encode-toolchain \
    && ln -s /opt/encode-toolchain/bin/versions.json /opt/encode-toolchain/bin/versions.txt /usr/local/share/encode-toolchain/
ENV PATH=/opt/encode-toolchain/bin:$PATH
RUN ffmpeg -hide_banner -version | head -1 && ffmpeg -hide_banner -filters | grep -q ' ass ' \
    && whisper-cli --help >/dev/null 2>&1 && cat /opt/encode-toolchain/bin/versions.txt
