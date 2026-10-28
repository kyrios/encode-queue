"""Qwen3-ForcedAligner-0.6B for encode-queue's align job (v0.2.16): the words of a text, timed in
the sound, in whisper-cli's `-oj` shape, one segment per word. The worker embeds this script and
runs it with the aligner's own Python: MLX and mlx-qwen3-asr on a Mac, PyTorch and qwen-asr on
Linux. Measured for iPhoneblog's captions in video-pipeline's docs/aligner-timing.md (#24).

    align.py version
    align.py run --model DIR --wav WAV --words WORDS.json --language de --out OUT.json
                 [--threads N] [--precision float32|lean]

The words file is {"words": [{"text", "start", "end"}]}: the text split on whitespace, with the
times it already has, which only cut the pieces. Progress goes to stderr as whisper-cli's
`progress = N%`.
"""

import argparse
import json
import math
import sys
import time

# A piece is at most 30 s: longer ones met the same onsets, but broke the aligner's order more
# often and took more memory (4.5 GiB at 300 s against 3.2 GiB, #24).
PIECE_S = 30.0
MAX_WORDS = 100_000
MAX_WORD_CHARS = 200
# The aligner's languages by whisper's codes. Japanese and Korean need tokenizers of their own
# (nagisa, soynlp) that the Mac's engine leaves out.
LANGUAGES = {"zh": "Chinese", "en": "English", "yue": "Cantonese", "fr": "French", "de": "German",
             "it": "Italian", "pt": "Portuguese", "ru": "Russian", "es": "Spanish"}
RATE = 16000


class Refused(Exception):
    """The job's input is not what the align job takes: exit code 3, never retried as a crash."""


def finite(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def read_words(doc):
    """The words of the words file, each {"text", "start", "end"} with finite times of at least 0;
    a time that runs back is held at the one before, so the pieces can be cut in order."""
    words = doc.get("words") if isinstance(doc, dict) else None
    if not isinstance(words, list) or not words:
        raise Refused("the words file needs a list of words")
    if len(words) > MAX_WORDS:
        raise Refused(f"{len(words)} words, more than {MAX_WORDS}")
    out, last = [], 0.0
    for n, w in enumerate(words):
        if not isinstance(w, dict):
            raise Refused(f"word {n} is not an object")
        text, start, end = w.get("text"), w.get("start"), w.get("end")
        if not isinstance(text, str) or not text or len(text) > MAX_WORD_CHARS or len(text.split()) != 1 \
                or text.strip() != text:
            raise Refused(f"word {n} is not one token of 1 to {MAX_WORD_CHARS} characters")
        if not finite(start) or not finite(end) or start < 0 or end < start:
            raise Refused(f"word {n} needs times with 0 <= start <= end")
        start = max(float(start), last)
        end = max(float(end), start)
        last = start
        out.append({"text": text, "start": start, "end": end})
    return out


def cut_pieces(words, duration, piece_s=PIECE_S):
    """The words as pieces [{"first", "last", "start", "end"}] (word indices and seconds of sound),
    as video-pipeline's bench cut them: a piece ends between two words, at the widest gap once it
    holds piece_s / 2, and its sound runs to the middle of that gap. Here a piece's sound is never
    longer than piece_s: across a long gap it starts or ends that much before its first word or
    after its last, and a word whose times span more than piece_s gets piece_s of sound."""
    pieces, i, n, lo = [], 0, len(words), 0.0
    while i < n:
        j = i
        while j + 1 < n and words[j + 1]["end"] - words[i]["start"] <= piece_s:
            j += 1
        last = j
        if j < n - 1:
            cands = [k for k in range(i, j + 1) if words[k]["end"] - words[i]["start"] >= piece_s / 2]
            if cands:
                last = max(cands, key=lambda k: (words[k + 1]["start"] - words[k]["end"], k))
        hi = duration if last == n - 1 else (words[last]["end"] + words[last + 1]["start"]) / 2
        hi = min(max(hi, lo), duration)
        a = min(words[i]["start"], duration)
        b = min(words[last]["end"], a + piece_s, duration)
        start = max(lo, a - (piece_s - (b - a)) / 2)
        end = min(hi, start + piece_s)
        start = max(lo, end - piece_s)
        pieces.append({"first": i, "last": last, "start": round(start, 3), "end": round(end, 3)})
        lo, i = hi, last + 1
    return pieces


def place(tokens, units, got, offset):
    """The times of a piece's tokens from the aligner's, as [(start, end) or None]: `units` is how
    many of the aligner's words each token makes (0 for punctuation alone), `got` the aligner's
    (start, end) per word in seconds of the piece, `offset` the piece's start."""
    if sum(units) != len(got):
        raise RuntimeError(f"the aligner timed {len(got)} words of {sum(units)}")
    out, at = [], 0
    for _, u in zip(tokens, units):
        if u == 0:
            out.append(None)
            continue
        part = got[at:at + u]
        at += u
        out.append((offset + part[0][0], offset + part[-1][1]))
    return out


def finish(words, times, duration):
    """Every word's (start, end) in seconds: the aligner's, held inside the sound and in order; a
    word without a time of its own sits, without length, where the word before it ends."""
    out, last = [], 0.0
    for w, t in zip(words, times):
        if t is None:
            s = e = last
        else:
            s = min(max(t[0], 0.0, out[-1][0] if out else 0.0), duration)
            e = min(max(t[1], s), duration)
        out.append((s, e))
        last = e
    return out


def stamp(ms):
    return f"{ms // 3600000:02d}:{ms // 60000 % 60:02d}:{ms // 1000 % 60:02d},{ms % 1000:03d}"


def whisper_doc(words, times, language, engine, pieces):
    """whisper-cli's `-oj` shape (video-pipeline's parse_whisper reads it): one segment per word,
    its text the word exactly after a space, its offsets in milliseconds."""
    segs = []
    for w, (s, e) in zip(words, times):
        a, b = int(round(s * 1000)), int(round(e * 1000))
        b = max(a, b)
        segs.append({"timestamps": {"from": stamp(a), "to": stamp(b)}, "offsets": {"from": a, "to": b},
                     "text": " " + w["text"]})
    return {"systeminfo": "", "model": {"type": "Qwen3-ForcedAligner-0.6B"},
            "params": {"model": "Qwen3-ForcedAligner-0.6B", "language": language},
            "result": {"language": language}, "engine": engine, "pieces": pieces, "transcription": segs}


# ------------------------------------------------------------------ the engines


class MLX:
    """mlx-qwen3-asr on a Mac's GPU, bfloat16 as published, MLX's buffer cache freed after each
    piece: without it a 21-minute episode in 30-second pieces grew to 24 GiB (#24)."""

    name = "mlx-qwen3-asr"

    def __init__(self):
        import mlx.core as mx
        import mlx_qwen3_asr
        from mlx_qwen3_asr.forced_aligner import ForcedAligner, ForcedAlignTextProcessor
        self.mx, self.version = mx, mlx_qwen3_asr.__version__
        self.backend = "mlx " + mx.__version__
        self._cls, self._text = ForcedAligner, ForcedAlignTextProcessor

    def load(self, model, threads, precision):
        self.aligner = self._cls(model, dtype=self.mx.bfloat16)
        self.aligner._ensure_loaded()
        self.precision = "bfloat16"

    def units(self, token, language):
        return len(self._text.tokenize_text(token, language))

    def align(self, clip, text, language):
        got = self.aligner.align(clip, text, language)
        self.mx.clear_cache()
        return [(g.start_time, g.end_time) for g in got]


class Torch:
    """qwen-asr on PyTorch's CPU. float32 by default: bfloat16 is six times slower on a CPU
    without bfloat16 arithmetic (encoder's Ryzen 5 3600). `lean` keeps the published bfloat16
    weights and casts each layer's weight to float32 for its product: 0.8 GB of its own memory
    and 1.7 GB of weights read from the model's file, against 4.4 GB, at four times float32's
    time, so it runs beside a 4K archive encode (measured on encoder, 2026-10-28)."""

    name = "qwen-asr"

    def __init__(self):
        import torch
        import qwen_asr
        from qwen_asr import Qwen3ForcedAligner
        self.torch, self._cls = torch, Qwen3ForcedAligner
        self.version = getattr(qwen_asr, "__version__", None) or _dist_version("qwen-asr")
        self.backend = "torch " + torch.__version__

    def load(self, model, threads, precision):
        torch = self.torch
        if threads > 0:
            torch.set_num_threads(threads)
        lean = precision == "lean"
        self.aligner = self._cls.from_pretrained(model, dtype=torch.bfloat16 if lean else torch.float32,
                                                 device_map="cpu")
        if lean:
            import torch.nn.functional as F

            def product(m):
                def forward(x):
                    b = None if m.bias is None else m.bias.float()
                    return F.linear(x.float(), m.weight.float(), b).to(x.dtype)
                return forward
            for mod in self.aligner.model.modules():
                if isinstance(mod, torch.nn.Linear):
                    mod.forward = product(mod)
        self.precision = "lean" if lean else "float32"

    def units(self, token, language):
        return len(self.aligner.aligner_processor.encode_timestamp(token, language)[0])

    def align(self, clip, text, language):
        items = self.aligner.align(audio=(clip, RATE), text=text, language=language)[0].items
        return [(i.start_time, i.end_time) for i in items]


def _dist_version(name):
    from importlib.metadata import version
    return version(name)


def engine():
    """The first engine this Python has: MLX, then PyTorch."""
    problems = []
    for cls in (MLX, Torch):
        try:
            return cls()
        except Exception as e:  # an engine that does not import is not there
            problems.append(f"{cls.name}: {type(e).__name__}: {e}")
    raise SystemExit("no aligner engine: " + "; ".join(problems))


# ------------------------------------------------------------------ the job


def read_wav(path):
    """16 kHz mono 16-bit PCM, as the worker's ffmpeg makes it (speechArgs), as float32."""
    import wave
    import numpy as np
    with wave.open(path) as w:
        if w.getframerate() != RATE or w.getnchannels() != 1 or w.getsampwidth() != 2:
            raise Refused(f"the sound is not 16 kHz mono 16-bit PCM ({w.getframerate()} Hz, "
                          f"{w.getnchannels()} channels, {8 * w.getsampwidth()} bits)")
        data = w.readframes(w.getnframes())
    return np.frombuffer(data, dtype="<i2").astype(np.float32) / 32768


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def run(a):
    if a.language not in LANGUAGES:
        raise Refused(f"language {a.language!r} is not one of {sorted(LANGUAGES)}")
    with open(a.words, encoding="utf-8") as f:
        words = read_words(json.load(f))
    audio = read_wav(a.wav)
    duration = len(audio) / RATE
    lang = LANGUAGES[a.language]
    pieces = cut_pieces(words, duration)
    try:
        # The aligner, never the archive's x265 beside it, is what the kernel ends when memory
        # runs out (Linux; a process may always raise its own).
        with open("/proc/self/oom_score_adj", "w") as f:
            f.write("1000")
    except OSError:
        pass
    t0 = time.monotonic()
    eng = engine()
    eng.load(a.model, a.threads, a.precision)
    log(f"{eng.name} {eng.version} ({eng.backend}, {eng.precision}): loaded in {time.monotonic() - t0:.1f} s, "
        f"{len(words)} words in {len(pieces)} pieces over {duration:.1f} s")
    times, stats = [None] * len(words), []
    for n, p in enumerate(pieces):
        t = time.monotonic()
        tokens = [w["text"] for w in words[p["first"]:p["last"] + 1]]
        units = [eng.units(tok, lang) for tok in tokens]
        clip = audio[int(p["start"] * RATE):int(p["end"] * RATE)]
        if any(units) and len(clip) >= RATE // 10:
            got = eng.align(clip, " ".join(tok for tok, u in zip(tokens, units) if u), lang)
            times[p["first"]:p["last"] + 1] = place(tokens, units, got, p["start"])
        # else: words past the sound's end, or punctuation alone, keep no time of their own
        stats.append({"start": p["start"], "end": p["end"], "words": len(tokens),
                      "s": round(time.monotonic() - t, 3)})
        log(f"progress = {int(100 * (n + 1) / len(pieces))}%")
    doc = whisper_doc(words, finish(words, times, duration), a.language,
                      {"name": eng.name, "version": eng.version, "backend": eng.backend,
                       "precision": eng.precision}, stats)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False)
    log(f"{len(words)} words timed in {time.monotonic() - t0:.1f} s")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("version", help="the engine's name and version, as JSON")
    r = sub.add_parser("run")
    r.add_argument("--model", required=True)
    r.add_argument("--wav", required=True)
    r.add_argument("--words", required=True)
    r.add_argument("--language", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--threads", type=int, default=0)
    r.add_argument("--precision", choices=("float32", "lean"), default="float32")
    a = ap.parse_args(argv)
    if a.cmd == "version":
        eng = engine()
        print(json.dumps({"name": eng.name, "version": eng.version, "backend": eng.backend}))
        return 0
    try:
        run(a)
    except Refused as e:
        log(f"refused: {e}")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
