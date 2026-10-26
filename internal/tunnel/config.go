package tunnel

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// ConfigMagic starts a tunnel configuration line (pipeline-admin.yaml, Enrolment.config_line).
const ConfigMagic = "eqtunnel1"

// Config is a worker's side of the tunnel, as the queue hands it out when the worker is added.
// Nothing in it is secret.
type Config struct {
	Address         netip.Prefix // the worker's tunnel address, a /32
	ServerPublicKey Key
	Endpoint        string // host:port of the queue's UDP
	AllowedIPs      netip.Prefix
	Keepalive       int
	API             string // the helper API's base URL inside the tunnel
}

// Line is the configuration as one line of text.
func (c Config) Line() string {
	return fmt.Sprintf("%s address=%s server_public_key=%s endpoint=%s allowed_ips=%s keepalive=%d api=%s",
		ConfigMagic, c.Address, c.ServerPublicKey, c.Endpoint, c.AllowedIPs, c.Keepalive, c.API)
}

// ParseLine reads a configuration line. Unknown keys are refused, so a typo shows at once.
func ParseLine(line string) (Config, error) {
	var c Config
	fields := strings.Fields(line)
	if len(fields) == 0 || fields[0] != ConfigMagic {
		return c, fmt.Errorf("a tunnel configuration starts with %q", ConfigMagic)
	}
	seen := map[string]bool{}
	for _, f := range fields[1:] {
		k, v, ok := strings.Cut(f, "=")
		if !ok || v == "" {
			return c, fmt.Errorf("%q is not key=value", f)
		}
		var err error
		switch k {
		case "address":
			c.Address, err = netip.ParsePrefix(v)
		case "server_public_key":
			c.ServerPublicKey, err = ParseKey(v)
		case "endpoint":
			c.Endpoint = v
		case "allowed_ips":
			c.AllowedIPs, err = netip.ParsePrefix(v)
		case "keepalive":
			c.Keepalive, err = strconv.Atoi(v)
		case "api":
			c.API = strings.TrimRight(v, "/")
		default:
			return c, fmt.Errorf("unknown key %q", k)
		}
		if err != nil {
			return c, fmt.Errorf("%s: %w", k, err)
		}
		seen[k] = true
	}
	for _, k := range []string{"address", "server_public_key", "endpoint", "allowed_ips", "api"} {
		if !seen[k] {
			return c, fmt.Errorf("missing %s", k)
		}
	}
	return c, nil
}
