package queue

// **The whisper models the queue knows** (v0.2.7): ggerganov/whisper.cpp on Hugging Face at the
// revision infra pins for encoder (roles/encoder/defaults/main.yml), by sha256 and size as that
// revision's tree lists them (read 2026-10-27). A worker fetches only these, from this URL, and
// checks the sha256. Quality is the order of Quality, best first.

// WhisperRevision is the pinned revision of ggerganov/whisper.cpp.
const WhisperRevision = "5359861c739e955e79d9a303bcbc70fb988958b1"

// CatalogueModel is one model a worker may be asked to fetch.
type CatalogueModel struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	URL    string `json:"url"`
	// PeakBytes is its expected RAM at its peak, for the extra slot.
	PeakBytes int64 `json:"-"`
	// Cost is its time against small's on the same machine, to guess its speed before it was
	// measured (whisper.cpp's own figures: medium about 2.5 times small; large-v3-turbo's four
	// decoder layers make it about 2 times small, on a GPU less).
	Cost float64 `json:"-"`
}

func hf(name string) string {
	return "https://huggingface.co/ggerganov/whisper.cpp/resolve/" + WhisperRevision + "/" + name
}

const (
	SHASmall        = "1be3a9b2063867b937e64e2ec7483364a79917e157fa98c5d94b5c1fffea987b"
	SHAMedium       = "6c14d5adee5f86394037b4e4e8b59f1673b6cee10e3cf0b11bbdbee79c156208"
	SHALargeV3Turbo = "1fc70f774d38eb169993ac391eea357ef47c88757ef72ee5943879b7e8e2bc69"
)

var catalogue = map[string]CatalogueModel{
	SHASmall: {Name: "ggml-small.bin", SHA256: SHASmall, Bytes: 487601967, URL: hf("ggml-small.bin"), PeakBytes: 1 << 30, Cost: 1},
	// PeakBytes of medium and turbo: measured on encoder on 2026-10-28 beside a 4K archive encode
	// (the cc-proofread session), turbo 1827 MB and medium 1867 MB at their peak, with a little
	// to spare: 2.0 GiB each (they were 3 and 2.5 GiB, guesses that held turbo back).
	SHAMedium:       {Name: "ggml-medium.bin", SHA256: SHAMedium, Bytes: 1533763059, URL: hf("ggml-medium.bin"), PeakBytes: 2 << 30, Cost: 2.5},
	SHALargeV3Turbo: {Name: "ggml-large-v3-turbo.bin", SHA256: SHALargeV3Turbo, Bytes: 1624555275, URL: hf("ggml-large-v3-turbo.bin"), PeakBytes: 2 << 30, Cost: 2},
}

// Quality is the catalogue's models, best first.
var Quality = []string{SHALargeV3Turbo, SHAMedium, SHASmall}
