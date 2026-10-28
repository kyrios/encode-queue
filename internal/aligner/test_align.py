"""align.py without an engine: the pieces, the placing of the aligner's words, the result's
shape. `python3 -m unittest test_align` here; aligner_test.go runs it."""

import json
import os
import subprocess
import sys
import tempfile
import unittest

import align


def words(spec):
    return [{"text": t, "start": s, "end": e} for t, s, e in spec]


class ReadWords(unittest.TestCase):
    def test_times_running_back_are_held(self):
        got = align.read_words({"words": words([("a", 1.0, 2.0), ("b", 0.5, 0.7), ("c", 3, 4)])})
        self.assertEqual([(w["start"], w["end"]) for w in got], [(1.0, 2.0), (1.0, 1.0), (3.0, 4.0)])

    def test_refusals(self):
        for doc in ({}, {"words": []}, {"words": [{"text": "a b", "start": 0, "end": 1}]},
                    {"words": [{"text": "", "start": 0, "end": 1}]},
                    {"words": [{"text": "a", "start": -1, "end": 1}]},
                    {"words": [{"text": "a", "start": 2, "end": 1}]},
                    {"words": [{"text": "a", "start": float("nan"), "end": 1}]},
                    {"words": [{"text": "a", "start": True, "end": 1}]},
                    {"words": ["a"]}):
            with self.assertRaises(align.Refused, msg=doc):
                align.read_words(doc)


class Pieces(unittest.TestCase):
    def test_a_preview_is_one_piece_to_the_end(self):
        ws = words([("a", 0.5, 1.0), ("b", 1.2, 2.0), ("c", 20.0, 26.0)])
        self.assertEqual(align.cut_pieces(ws, 26.8), [{"first": 0, "last": 2, "start": 0.0, "end": 26.8}])

    def test_cut_at_the_widest_gap_in_the_second_half(self):
        # one word a second, a 0.9 s gap after the word ending at 20.1, small ones elsewhere
        spec, t = [], 0.0
        for k in range(60):
            spec.append((f"w{k}", t, t + 0.8))
            t += 1.7 if k == 19 else 1.0
        ws = words(spec)
        pieces = align.cut_pieces(ws, t + 1)
        self.assertEqual(pieces[0]["last"], 19)
        self.assertAlmostEqual(pieces[0]["end"], (ws[19]["end"] + ws[20]["start"]) / 2)
        for p, q in zip(pieces, pieces[1:]):
            self.assertEqual(q["first"], p["last"] + 1)
            self.assertLessEqual(p["end"], q["start"])
        for p in pieces:
            self.assertLessEqual(p["end"] - p["start"], align.PIECE_S + 1e-9)
            self.assertLessEqual(p["start"], ws[p["first"]]["start"])
            self.assertGreaterEqual(p["end"], ws[p["last"]]["end"])

    def test_never_longer_than_30_s_across_long_gaps(self):
        ws = words([("a", 0.0, 1.0), ("b", 100.0, 101.0), ("c", 102.0, 103.0), ("d", 300.0, 400.0)])
        pieces = align.cut_pieces(ws, 500.0)
        self.assertEqual([(p["first"], p["last"]) for p in pieces], [(0, 0), (1, 2), (3, 3)])
        for p, w in zip(pieces, (ws[0], ws[1], ws[3])):
            self.assertLessEqual(p["end"] - p["start"], 30.0 + 1e-9)
            self.assertLessEqual(p["start"], w["start"])
        self.assertEqual(pieces[2]["start"], 300.0)  # a word of 100 s gets its first 30
        self.assertEqual(pieces[2]["end"], 330.0)

    def test_words_past_the_sounds_end(self):
        ws = words([("a", 0.0, 1.0), ("b", 9.0, 12.0)])
        self.assertEqual(align.cut_pieces(ws, 5.0), [{"first": 0, "last": 1, "start": 0.0, "end": 5.0}])


class Place(unittest.TestCase):
    def test_tokens_take_their_units(self):
        got = [(0.1, 0.2), (0.3, 0.4), (0.5, 0.6), (0.7, 0.9)]
        self.assertEqual(align.place(["Hallo", "–", "Ab-", "c"], [1, 0, 2, 1], got, 10.0),
                         [(10.1, 10.2), None, (10.3, 10.6), (10.7, 10.9)])
        with self.assertRaises(RuntimeError):
            align.place(["a"], [1], [], 0.0)

    def test_finish_keeps_order_and_sound(self):
        ws = words([("a", 0, 0), ("–", 0, 0), ("b", 0, 0), ("c", 0, 0)])
        got = align.finish(ws, [(1.0, 1.5), None, (0.8, 2.0), (4.0, 9.0)], 5.0)
        self.assertEqual(got, [(1.0, 1.5), (1.5, 1.5), (1.5, 2.0), (4.0, 5.0)])


class Shape(unittest.TestCase):
    def test_whisper_cli_json(self):
        ws = words([("Ein", 0, 0), ("Thema,", 0, 0)])
        doc = align.whisper_doc(ws, [(0.48, 0.8), (3661.0004, 3661.5)], "de", {"name": "x"}, [])
        self.assertEqual(doc["transcription"][0], {"timestamps": {"from": "00:00:00,480", "to": "00:00:00,800"},
                                                   "offsets": {"from": 480, "to": 800}, "text": " Ein"})
        self.assertEqual(doc["transcription"][1]["timestamps"]["from"], "01:01:01,000")
        self.assertEqual(doc["result"], {"language": "de"})


class Command(unittest.TestCase):
    def test_a_bad_words_file_is_refused_with_3(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "words.json")
            with open(p, "w") as f:
                json.dump({"words": [{"text": "a b", "start": 0, "end": 1}]}, f)
            r = subprocess.run([sys.executable, align.__file__, "run", "--model", d, "--wav", p, "--words", p,
                                "--language", "de", "--out", os.path.join(d, "o.json")], capture_output=True, text=True)
            self.assertEqual(r.returncode, 3, r.stderr)
            self.assertIn("refused: word 0", r.stderr)
            r = subprocess.run([sys.executable, align.__file__, "run", "--model", d, "--wav", p, "--words", p,
                                "--language", "ja", "--out", os.path.join(d, "o.json")], capture_output=True, text=True)
            self.assertEqual(r.returncode, 3, r.stderr)


if __name__ == "__main__":
    unittest.main()
