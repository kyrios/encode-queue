package queue

import (
	"fmt"
	"strconv"
	"strings"
)

// **What a job makes, for Funkhaus's Encoder page** (v0.2.19; Thorsten, 2026-10-28): its type in
// the page's words and one line of detail, "Video" and "1080p · slower CRF 29". Thumbnails, Peaks
// and Audio name jobs the queue does not run yet: Funkhaus makes peaks itself, and nothing
// submits the other two.
const (
	TypeVideo      = "Video"
	TypeAudio      = "Audio"
	TypeThumbnails = "Thumbnails"
	TypeCaptions   = "Untertitel"
	TypePeaks      = "Peaks"
	TypeAlign      = "Align"
	TypeCut        = "Cut"
	TypeRemux      = "Remux"
	TypeCheck      = "Check"
)

// jobType is the job's type and its detail; a kind the queue does not know is its own type.
func jobType(j *Job) (string, string) {
	switch {
	case j.rung != nil:
		r := j.rung
		codec := ""
		if r.Codec == "libx264" {
			codec = " H.264"
		}
		return TypeVideo, fmt.Sprintf("%dp%s · %s CRF %s", min(r.Width, r.Height), codec, r.Preset, number(r.CRF))
	case j.whisper != nil:
		return TypeCaptions, shortModel(j.whisper.acceptable()[0], j.whisper.Model)
	case j.align != nil:
		return TypeAlign, strings.TrimSpace(shortModel(j.align.ModelSHA256, j.align.Model) + " · " + j.align.Language)
	case j.cut != nil:
		c := j.cut
		return TypeCut, fmt.Sprintf("%s–%s s", number(c.StartS), number(c.StartS+c.LengthS))
	case j.remux != nil:
		if j.remux.HVC1Tag {
			return TypeRemux, "hvc1"
		}
		return TypeRemux, ""
	case j.check != nil:
		return TypeCheck, checkDetail(j.check.Check)
	}
	if k := j.Spec.Kind; k != "" {
		return strings.ToUpper(k[:1]) + k[1:], ""
	}
	return "", ""
}

// shortModel is a model's name as the page shows it: "turbo", "medium", "small".
func shortModel(sha, given string) string {
	name := given
	if name == "" {
		name = modelLabel(sha)
	}
	name = strings.TrimSuffix(strings.TrimPrefix(name, "ggml-"), ".bin")
	if strings.HasSuffix(name, "-turbo") {
		return "turbo"
	}
	return name
}

// checkDetail names what a check job measures: "SSIM, PSNR-Y, VMAF · every 5th frame", or its
// decode and frame checks.
func checkDetail(c *CheckSettings) string {
	if c == nil {
		return ""
	}
	var parts []string
	if c.Compare != nil {
		var ms []string
		for _, m := range c.Compare.Metrics {
			ms = append(ms, strings.ToUpper(strings.ReplaceAll(m, "_y", "-Y")))
		}
		if c.Compare.VMAF != nil {
			ms = append(ms, "VMAF")
		}
		p := strings.Join(ms, ", ")
		if c.Compare.Every > 1 {
			p += fmt.Sprintf(" · every %s frame", ordinal(c.Compare.Every))
		}
		parts = append(parts, p)
	}
	if c.Decode != nil && *c.Decode {
		parts = append(parts, "decode")
	}
	if c.Frames != nil && *c.Frames {
		parts = append(parts, "frames")
	}
	if c.Framemd5 {
		parts = append(parts, "framemd5")
	}
	return strings.Join(parts, " · ")
}

func number(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

func ordinal(n int) string {
	switch {
	case n%100 >= 11 && n%100 <= 13:
		return strconv.Itoa(n) + "th"
	case n%10 == 1:
		return strconv.Itoa(n) + "st"
	case n%10 == 2:
		return strconv.Itoa(n) + "nd"
	case n%10 == 3:
		return strconv.Itoa(n) + "rd"
	}
	return strconv.Itoa(n) + "th"
}
