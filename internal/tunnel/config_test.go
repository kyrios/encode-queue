package tunnel

import "testing"

func TestConfigLine(t *testing.T) {
	k, _ := GenerateKey()
	line := "eqtunnel1 address=10.10.20.11/32 server_public_key=" + k.Public().String() +
		" endpoint=vpn.example.org:51821 allowed_ips=10.10.20.1/32 keepalive=25 api=http://10.10.20.1:8090/v1"
	c, err := ParseLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if c.Line() != line {
		t.Fatalf("round trip:\n%s\n%s", c.Line(), line)
	}
	for _, bad := range []string{"", "wg address=1", line + " extra=1", "eqtunnel1 address=10.10.20.11/32"} {
		if _, err := ParseLine(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if k.Public() == k || k.Public().Public() == k.Public() {
		t.Fatal("public key equals its private key")
	}
}
