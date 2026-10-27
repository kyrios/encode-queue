#!/usr/bin/env bash
# Builds the toolchain of toolchain/versions into PREFIX/bin: ffmpeg and ffprobe (libx265 with
# 8, 10 and 12 bits, libx264, libvmaf, libmp3lame, and libass), fc-list, whisper-cli (Metal on
# macOS), and versions.txt and versions.json with each tool's version string as the worker
# reads it. PREFIX/fonts holds Inter and fonts.conf, which sees only those fonts: point
# FONTCONFIG_FILE at it, and libass picks the same font on every machine. PREFIX/licenses gets each component's licence from its source and SOURCES.md:
# ffmpeg, built with --enable-gpl --enable-version3, is GPLv3, and whoever ships it ships these
# and points to the source (the toolchain release attaches the tarballs).
#
#   toolchain/build.sh PREFIX [WORKDIR]
#
# On Linux (the image's build stage) it needs build-essential, nasm, cmake, meson, ninja, xxd,
# pkg-config, curl, git (x265 reads its version file only when git is found), and libass-dev
# and zlib1g-dev; on macOS Xcode's tools, cmake, meson, ninja
# and pkg-config. Nothing is installed outside PREFIX and WORKDIR.
set -euo pipefail
# macOS runs bash 3.2: no empty "${a[@]}" under set -u, no mapfile, no associative arrays.
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=versions
. "$here/versions"
prefix=$1
work=${2:-$(mktemp -d)}
deps=$work/deps
mkdir -p "$prefix/bin" "$work/src" "$deps"
prefix=$(cd "$prefix" && pwd)
os=$(uname -s)
if [ "$os" = Darwin ]; then
  jobs=$(sysctl -n hw.ncpu)
  export MACOSX_DEPLOYMENT_TARGET=14.0
  cxxlib=-lc++
else
  jobs=$(nproc)
  cxxlib=-lstdc++
fi
export PKG_CONFIG_PATH=$deps/lib/pkgconfig
# x265's CMakeLists asks for cmake 2.8.8...3.10; cmake 4 refuses that without this.
cmake_compat=-DCMAKE_POLICY_VERSION_MINIMUM=3.5

fetch() { # url file sha256
  local f=$work/src/$2
  [ -f "$f" ] || curl -fsSL --retry 3 -o "$f" "$1"
  echo "$3  $f" | shasum -a 256 -c --quiet 2>/dev/null || echo "$3  $f" | sha256sum -c --quiet
}
unpack() { # file dir
  rm -rf "$work/$2" && mkdir -p "$work/$2"
  tar xf "$work/src/$1" -C "$work/$2" --strip-components 1
}
fetch "$FFMPEG_URL" ffmpeg.tar.xz "$FFMPEG_SHA256"
fetch "$X265_URL" x265.tar.gz "$X265_SHA256"
fetch "$X264_URL" x264.tar.gz "$X264_SHA256"
fetch "$VMAF_URL" vmaf.tar.gz "$VMAF_SHA256"
fetch "$LAME_URL" lame.tar.gz "$LAME_SHA256"
fetch "$WHISPER_URL" whisper.tar.gz "$WHISPER_SHA256"
fetch "$EXPAT_URL" expat.tar.xz "$EXPAT_SHA256"
fetch "$FREETYPE_URL" freetype.tar.xz "$FREETYPE_SHA256"
fetch "$FRIBIDI_URL" fribidi.tar.xz "$FRIBIDI_SHA256"
fetch "$HARFBUZZ_URL" harfbuzz.tar.xz "$HARFBUZZ_SHA256"
fetch "$FONTCONFIG_URL" fontconfig.tar.xz "$FONTCONFIG_SHA256"
fetch "$LIBASS_URL" libass.tar.xz "$LIBASS_SHA256"
fetch "$INTER_URL" inter.zip "$INTER_SHA256"

# The caption stack, static, from source on both platforms, so libass, its shaping and its font
# matching are the same versions everywhere (no system libraries, no CoreText).
mesonlib() { # dir options...
  local d=$1
  shift
  (cd "$work/$d" && meson setup build --prefix="$deps" --libdir=lib --buildtype=release \
    --default-library=static --wrap-mode=nodownload "$@" >/dev/null && ninja -C build >/dev/null \
    && ninja -C build install >/dev/null)
}
echo "== expat $EXPAT_VERSION"
unpack expat.tar.xz expat
(cd "$work/expat" && cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$deps" \
  -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DEXPAT_SHARED_LIBS=OFF \
  -DEXPAT_BUILD_TOOLS=OFF -DEXPAT_BUILD_EXAMPLES=OFF -DEXPAT_BUILD_TESTS=OFF -DEXPAT_BUILD_DOCS=OFF \
  >/dev/null && cmake --build build -j"$jobs" >/dev/null && cmake --install build >/dev/null)
echo "== freetype $FREETYPE_VERSION"
unpack freetype.tar.xz freetype
mesonlib freetype -Dbrotli=disabled -Dbzip2=disabled -Dharfbuzz=disabled -Dpng=disabled -Dzlib=disabled
echo "== fribidi $FRIBIDI_VERSION"
unpack fribidi.tar.xz fribidi
mesonlib fribidi -Ddocs=false -Dbin=false -Dtests=false
echo "== harfbuzz $HARFBUZZ_VERSION"
unpack harfbuzz.tar.xz harfbuzz
mesonlib harfbuzz -Dfreetype=enabled -Dglib=disabled -Dgobject=disabled -Dcairo=disabled -Dchafa=disabled \
  -Dicu=disabled -Dgraphite2=disabled -Dcoretext=disabled -Dintrospection=disabled -Dtests=disabled \
  -Ddocs=disabled -Dutilities=disabled -Dbenchmark=disabled
echo "== fontconfig $FONTCONFIG_VERSION"
unpack fontconfig.tar.xz fontconfig
mesonlib fontconfig -Dxml-backend=expat -Dtools=enabled -Dtests=disabled -Ddoc=disabled -Dnls=disabled \
  -Dcache-build=disabled -Diconv=disabled
echo "== libass $LIBASS_VERSION"
unpack libass.tar.xz libass
mesonlib libass -Dfontconfig=enabled -Dcoretext=disabled -Ddirectwrite=disabled -Dtest=disabled \
  -Dcompare=disabled -Dprofile=disabled -Dfuzz=disabled -Dcheckasm=disabled

echo "== x265 $X265_VERSION: 8-bit main with the 10- and 12-bit libraries linked in"
unpack x265.tar.gz x265
b=$work/x265/build/multi
mkdir -p "$b/12bit" "$b/10bit" "$b/8bit"
(cd "$b/12bit" && cmake ../../../source $cmake_compat -DCMAKE_BUILD_TYPE=Release -DHIGH_BIT_DEPTH=ON -DMAIN12=ON \
  -DEXPORT_C_API=OFF -DENABLE_SHARED=OFF -DENABLE_CLI=OFF >/dev/null && make -s -j"$jobs")
(cd "$b/10bit" && cmake ../../../source $cmake_compat -DCMAKE_BUILD_TYPE=Release -DHIGH_BIT_DEPTH=ON \
  -DEXPORT_C_API=OFF -DENABLE_SHARED=OFF -DENABLE_CLI=OFF >/dev/null && make -s -j"$jobs")
cd "$b/8bit"
ln -sf ../10bit/libx265.a libx265_main10.a
ln -sf ../12bit/libx265.a libx265_main12.a
cmake ../../../source $cmake_compat -DCMAKE_BUILD_TYPE=Release -DEXTRA_LIB="x265_main10.a;x265_main12.a" \
  -DEXTRA_LINK_FLAGS=-L. -DLINKED_10BIT=ON -DLINKED_12BIT=ON -DENABLE_SHARED=OFF -DENABLE_CLI=OFF \
  -DCMAKE_INSTALL_PREFIX="$deps" >/dev/null
make -s -j"$jobs"
mv libx265.a libx265_main.a
if [ "$os" = Darwin ]; then
  libtool -static -o libx265.a libx265_main.a libx265_main10.a libx265_main12.a
else
  printf 'CREATE libx265.a\nADDLIB libx265_main.a\nADDLIB libx265_main10.a\nADDLIB libx265_main12.a\nSAVE\nEND\n' | ar -M
fi
make -s install >/dev/null

echo "== x264 r$X264_REV"
unpack x264.tar.gz x264
cd "$work/x264"
./configure --prefix="$deps" --enable-static --disable-cli --enable-pic --bit-depth=all --chroma-format=all >/dev/null
api=$(sed -n 's/^#define X264_BUILD \([0-9]*\).*/\1/p' x264.h)
short=$(echo "$X264_COMMIT" | cut -c1-7)
# configure appended version.sh's lines to x264_config.h, empty without git; ours replace them.
sed -i.bak -E '/X264_(REV|VERSION|POINTVER)/d' x264_config.h
cat >> x264_config.h <<V
#define X264_REV $X264_REV
#define X264_REV_DIFF 0
#define X264_VERSION " r$X264_REV $short"
#define X264_POINTVER "0.$api.$X264_REV $short"
V
make -s -j"$jobs"
make -s install >/dev/null

echo "== libvmaf $VMAF_VERSION"
unpack vmaf.tar.gz vmaf
cd "$work/vmaf/libvmaf"
meson setup build --prefix="$deps" --libdir=lib --buildtype=release --default-library=static \
  -Denable_tests=false -Denable_docs=false -Denable_tools=false -Dbuilt_in_models=true >/dev/null
ninja -C build >/dev/null
ninja -C build install >/dev/null

echo "== lame $LAME_VERSION"
unpack lame.tar.gz lame
cd "$work/lame"
# lame_init_old is listed for export but not built, which breaks the static link on macOS.
sed -i.bak '/lame_init_old/d' include/libmp3lame.sym
./configure --prefix="$deps" --enable-static --disable-shared --disable-frontend --disable-decoder \
  --disable-dependency-tracking >/dev/null
make -s -j"$jobs"
make -s install >/dev/null

echo "== ffmpeg $FFMPEG_VERSION"
unpack ffmpeg.tar.xz ffmpeg
cd "$work/ffmpeg"
extra=(--enable-libass --enable-libfontconfig --enable-libfreetype --enable-libfribidi --enable-libharfbuzz)
./configure --prefix="$prefix" --enable-gpl --enable-version3 --disable-autodetect --enable-zlib \
  --enable-libx264 --enable-libx265 --enable-libvmaf --enable-libmp3lame ${extra[@]+"${extra[@]}"} \
  --disable-doc --disable-ffplay --disable-debug --pkg-config-flags=--static \
  --extra-cflags="-I$deps/include" --extra-ldflags="-L$deps/lib" --extra-libs="$cxxlib -lm -lpthread" >/dev/null \
  || { tail -40 ffbuild/config.log; exit 1; }
make -s -j"$jobs"
install -m 0755 ffmpeg ffprobe "$prefix/bin/"

echo "== whisper.cpp $WHISPER_VERSION"
unpack whisper.tar.gz whisper
cd "$work/whisper"
wargs=(-DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DGGML_NATIVE=OFF -DGGML_CCACHE=OFF
  -DWHISPER_BUILD_TESTS=OFF -DWHISPER_BUILD_SERVER=OFF -DWHISPER_SDL2=OFF -DWHISPER_CURL=OFF)
if [ "$os" = Darwin ]; then
  wargs+=(-DGGML_METAL=ON -DGGML_METAL_EMBED_LIBRARY=ON)
elif [ "$(uname -m)" = x86_64 ]; then
  # x86-64-v3, so the image runs on any machine since Haswell, encoder included.
  wargs+=(-DGGML_AVX=ON -DGGML_AVX2=ON -DGGML_FMA=ON -DGGML_F16C=ON -DGGML_BMI2=ON)
fi
cmake -B build "${wargs[@]}" >/dev/null
cmake --build build -j"$jobs" --target whisper-cli >/dev/null
install -m 0755 build/bin/whisper-cli "$prefix/bin/"
# The worker lists the fonts with fc-list: the toolchain's own, built with its fontconfig.
install -m 0755 "$deps/bin/fc-list" "$prefix/bin/"

echo "== fonts: Inter $INTER_VERSION and a fonts.conf that sees only them"
rm -rf "$prefix/fonts" && mkdir -p "$prefix/fonts" "$work/inter"
(cd "$work/inter" && unzip -q -o "$work/src/inter.zip")
cp "$work/inter"/extras/ttf/Inter-*.ttf "$prefix/fonts/"
cat > "$prefix/fonts/fonts.conf" <<'F'
<?xml version="1.0"?>
<!DOCTYPE fontconfig SYSTEM "urn:fontconfig:fonts.dtd">
<!-- encode-toolchain: the toolchain's fonts and no others, so libass takes the same font on
     every machine. FONTCONFIG_FILE points here; the paths are relative to this file. -->
<fontconfig>
  <dir prefix="relative">.</dir>
  <cachedir prefix="xdg">encode-toolchain-fontconfig</cachedir>
</fontconfig>
F

echo "== licences"
lic=$prefix/licenses
rm -rf "$lic" && mkdir -p "$lic"
cp "$work/ffmpeg/COPYING.GPLv3" "$lic/ffmpeg-COPYING.GPLv3"
cp "$work/ffmpeg/LICENSE.md" "$lic/ffmpeg-LICENSE.md"
cp "$work/x265/COPYING" "$lic/x265-COPYING"
cp "$work/x264/COPYING" "$lic/x264-COPYING"
cp "$work/vmaf/LICENSE" "$lic/libvmaf-LICENSE"
cp "$work/lame/COPYING" "$lic/lame-COPYING"
cp "$work/lame/LICENSE" "$lic/lame-LICENSE"
# whisper.cpp's licence covers ggml, which it carries.
cp "$work/whisper/LICENSE" "$lic/whisper.cpp-LICENSE"
cp "$work/libass/COPYING" "$lic/libass-COPYING"
cp "$work/harfbuzz/COPYING" "$lic/harfbuzz-COPYING"
cp "$work/fribidi/COPYING" "$lic/fribidi-COPYING"
# FreeType offers the FTL or GPLv2 or later; both suit a GPLv3 ffmpeg. Both ship.
cp "$work/freetype/LICENSE.TXT" "$lic/freetype-LICENSE.TXT"
cp "$work/freetype/docs/FTL.TXT" "$lic/freetype-FTL.TXT"
cp "$work/freetype/docs/GPLv2.TXT" "$lic/freetype-GPLv2.TXT"
cp "$work/fontconfig/COPYING" "$lic/fontconfig-COPYING"
cp "$work/expat/COPYING" "$lic/expat-COPYING"
cp "$work/inter/LICENSE.txt" "$lic/Inter-LICENSE.txt"
config=$("$prefix/bin/ffmpeg" -hide_banner -buildconf 2>/dev/null | sed -n 's/^ *\(--.*\)/\1/p' | tr '\n' ' ')
if [ "$os" = Darwin ]; then
  system="zlib and the C++ runtime are macOS's own; nothing else is linked."
else
  system="zlib, the C++ runtime and libgomp are Debian 13's, linked from the image; their copyright files are in /usr/share/doc/<package>/copyright there."
fi
cat > "$lic/SOURCES.md" <<S
# encode-toolchain $TOOLCHAIN: what is in it and where its source is

ffmpeg and ffprobe are built with \`--enable-gpl --enable-version3\` and are therefore under
the **GNU GPL version 3** (ffmpeg-COPYING.GPLv3, ffmpeg-LICENSE.md); the libraries linked into
them keep their own licences, in this folder. whisper-cli is MIT.

**The Corresponding Source** is attached to the release \`toolchain-$TOOLCHAIN\` of
https://github.com/kyrios/encode-queue: the tarballs below, byte for byte, and the build
script \`toolchain/build.sh\` with \`toolchain/versions\` at that tag.

| component | version | licence | file here | source | sha256 |
|---|---|---|---|---|---|
| ffmpeg, ffprobe | $FFMPEG_VERSION | GPL-3.0-or-later (as built) | ffmpeg-COPYING.GPLv3, ffmpeg-LICENSE.md | $FFMPEG_URL | $FFMPEG_SHA256 |
| x265 | $X265_VERSION | GPL-2.0-or-later | x265-COPYING | $X265_URL | $X265_SHA256 |
| x264 | r$X264_REV ($X264_COMMIT) | GPL-2.0-or-later | x264-COPYING | $X264_URL | $X264_SHA256 |
| libvmaf | $VMAF_VERSION | BSD-2-Clause-Patent | libvmaf-LICENSE | $VMAF_URL | $VMAF_SHA256 |
| LAME (libmp3lame) | $LAME_VERSION | LGPL-2.0-or-later | lame-COPYING, lame-LICENSE | $LAME_URL | $LAME_SHA256 |
| whisper.cpp (with ggml) | $WHISPER_VERSION | MIT | whisper.cpp-LICENSE | $WHISPER_URL | $WHISPER_SHA256 |
| libass | $LIBASS_VERSION | ISC | libass-COPYING | $LIBASS_URL | $LIBASS_SHA256 |
| HarfBuzz | $HARFBUZZ_VERSION | MIT (Old MIT) | harfbuzz-COPYING | $HARFBUZZ_URL | $HARFBUZZ_SHA256 |
| GNU FriBidi | $FRIBIDI_VERSION | LGPL-2.1-or-later | fribidi-COPYING | $FRIBIDI_URL | $FRIBIDI_SHA256 |
| FreeType | $FREETYPE_VERSION | FTL or GPL-2.0-or-later | freetype-LICENSE.TXT, freetype-FTL.TXT, freetype-GPLv2.TXT | $FREETYPE_URL | $FREETYPE_SHA256 |
| fontconfig (with fc-list) | $FONTCONFIG_VERSION | MIT-style | fontconfig-COPYING | $FONTCONFIG_URL | $FONTCONFIG_SHA256 |
| Expat | $EXPAT_VERSION | MIT | expat-COPYING | $EXPAT_URL | $EXPAT_SHA256 |
| Inter (fonts/) | $INTER_VERSION | OFL-1.1 | Inter-LICENSE.txt | $INTER_URL | $INTER_SHA256 |

Portions of this software are copyright © 2026 The FreeType Project (https://freetype.org).
All rights reserved.

System libraries: $system

ffmpeg's configuration, as \`ffmpeg -buildconf\` prints it:

\`\`\`
$config
\`\`\`
S

echo "== versions"
cd "$prefix/bin"
frame() { ./ffmpeg -hide_banner -nostdin -loglevel error -f lavfi -i color=c=black:s=64x64:d=0.04 -frames:v 1 "$@" -; }
ffv=$(./ffmpeg -version | sed -n 's/^ffmpeg version \([^ ]*\).*/\1/p')
x265v=$(frame -c:v libx265 -x265-params log-level=error -f hevc | LC_ALL=C grep -ao 'x265 (build [0-9]*) - [^:]*' | sed 's/.* - //')
x264v=$(frame -c:v libx264 -f h264 | LC_ALL=C grep -ao 'x264 - core [0-9]* r[0-9]* [0-9a-f]*' | sed 's/^x264 - //')
cat > versions.txt <<V
toolchain $TOOLCHAIN
ffmpeg $ffv
libx265 $x265v
libx264 $x264v
libvmaf $VMAF_VERSION
lame $LAME_VERSION
whisper.cpp $WHISPER_VERSION
libass $LIBASS_VERSION
V
# The same as JSON in helper.yaml's Tool form, for the pipeline's manifests.
printf '[\n  {"name": "ffmpeg", "version": "%s"},\n  {"name": "libx265", "version": "%s"},\n  {"name": "libx264", "version": "%s"},\n  {"name": "libvmaf", "version": "%s"},\n  {"name": "lame", "version": "%s"},\n  {"name": "whisper.cpp", "version": "%s"},\n  {"name": "libass", "version": "%s"}\n]\n' \
  "$ffv" "$x265v" "$x264v" "$VMAF_VERSION" "$LAME_VERSION" "$WHISPER_VERSION" "$LIBASS_VERSION" > versions.json
cat versions.txt versions.json
for v in "$ffv" "$x265v" "$x264v"; do
  case $v in ''|unknown*) echo "a version string is empty or unknown" >&2; exit 1 ;; esac
done
# Everything the worker and video-pipeline call (its list, 2026-10-26).
need() { # what names...
  local what=$1 list
  shift
  list=$(./ffmpeg -hide_banner "-$what")
  for n in "$@"; do
    # A name stands after the flags, alone or in a list (mov,mp4,...).
    # A here-string, not a pipe: grep -q stops reading early, and under pipefail the writer's
    # broken pipe would count as a miss.
    grep -Eq "^ *[A-Z.]+( [a-z.])? +([^ ]+,)?$n(,[^ ]+)?( |$)" <<<"$list" || { echo "ffmpeg lacks $what $n" >&2; exit 1; }
  done
}
filters=(scale select setpts fps format split crop fade afade pan astats ssim psnr xpsnr libvmaf ass subtitles)
need filters "${filters[@]}"
need encoders libx264 libx265 aac pcm_s16le libmp3lame
need muxers mp4 mov wav mp3 null framemd5 hevc h264
need demuxers mov wav mp3 lavfi
need decoders h264 hevc prores aac alac mp3 pcm_s16le
need filters testsrc testsrc2 sine color
./ffmpeg -hide_banner -nostdin -loglevel error -f lavfi -i testsrc2=s=320x180:d=0.2 -frames:v 4 \
  -c:v libx265 -pix_fmt yuv420p10le -x265-params log-level=error -f null - || { echo "no 10-bit x265" >&2; exit 1; }
./whisper-cli --help >/dev/null 2>&1 || { echo "whisper-cli does not start" >&2; exit 1; }
# Captions: with the toolchain's fonts.conf, libass takes Inter ExtraBold, as the pipeline asks.
export FONTCONFIG_FILE="$prefix/fonts/fonts.conf"
./fc-list : family | grep -q '^Inter' || { echo "fc-list does not see Inter" >&2; exit 1; }
took=$(./ffmpeg -hide_banner -nostdin -loglevel verbose -f lavfi -i color=c=0x203040:s=1080x1920:d=0.04 \
  -frames:v 1 -vf "ass=filename=$here/check/captions.ass" -f null - 2>&1 | grep 'fontselect' || true)
echo "$took"
echo "$took" | grep -q 'Inter-ExtraBold' || { echo "libass did not take Inter-ExtraBold" >&2; exit 1; }
if [ "$os" = Darwin ]; then
  for f in ffmpeg ffprobe whisper-cli; do
    if otool -L "$f" | tail -n +2 | grep -v -E '^\s*(/usr/lib/|/System/)'; then
      echo "$f links a library outside the system" >&2; exit 1
    fi
  done
fi
