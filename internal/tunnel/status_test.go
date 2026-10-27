package tunnel

import (
	"testing"
	"time"
)

func TestParsePeerStatus(t *testing.T) {
	a, b := Key{1}, Key{2}
	conf := "private_key=00\nlisten_port=51821\n" +
		"public_key=" + a.Hex() + "\nendpoint=203.0.113.7:51820\nlast_handshake_time_sec=1793091600\nlast_handshake_time_nsec=5\nallowed_ip=10.10.20.11/32\n" +
		"public_key=" + b.Hex() + "\nlast_handshake_time_sec=0\nlast_handshake_time_nsec=0\n"
	got := parsePeerStatus(conf)
	if len(got) != 2 {
		t.Fatalf("%d peers", len(got))
	}
	if p := got[a]; p.Endpoint != "203.0.113.7:51820" || !p.LastHandshake.Equal(time.Unix(1793091600, 5)) {
		t.Fatalf("a: %+v", p)
	}
	if p := got[b]; p.Endpoint != "" || !p.LastHandshake.IsZero() {
		t.Fatalf("b, before a handshake: %+v", p)
	}
}
