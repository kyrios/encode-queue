package tunnel

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

// MTU leaves room for WireGuard's overhead on a 1500-byte path.
const MTU = 1420

func logger(log *slog.Logger, prefix string) *device.Logger {
	return &device.Logger{
		Verbosef: func(format string, args ...any) { log.Debug(prefix + fmt.Sprintf(format, args...)) },
		Errorf:   func(format string, args ...any) { log.Warn(prefix + fmt.Sprintf(format, args...)) },
	}
}

// Peer is one worker on the queue's side.
type Peer struct {
	PublicKey Key
	Address   netip.Addr
}

// Door is the queue's end: one UDP port, one address, and its own TCP/IP stack in which the
// helper API listens.
type Door struct {
	dev   *device.Device
	net   *netstack.Net
	addr  netip.Addr
	mu    sync.Mutex
	peers map[Key]netip.Addr
}

// OpenDoor brings the queue's WireGuard up on listenPort (UDP, all of the host's addresses)
// with the tunnel address addr.
func OpenDoor(priv Key, addr netip.Addr, listenPort int, log *slog.Logger) (*Door, error) {
	tdev, tnet, err := netstack.CreateNetTUN([]netip.Addr{addr}, nil, MTU)
	if err != nil {
		return nil, err
	}
	dev := device.NewDevice(tdev, conn.NewDefaultBind(), logger(log, "wireguard: "))
	if err := dev.IpcSet(fmt.Sprintf("private_key=%s\nlisten_port=%d\n", priv.Hex(), listenPort)); err != nil {
		dev.Close()
		return nil, err
	}
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, err
	}
	return &Door{dev: dev, net: tnet, addr: addr, peers: map[Key]netip.Addr{}}, nil
}

// Listen opens a TCP listener on the tunnel address.
func (d *Door) Listen(port int) (net.Listener, error) {
	return d.net.ListenTCPAddrPort(netip.AddrPortFrom(d.addr, uint16(port)))
}

// SetPeers makes the peer list exactly peers, adding and removing only what changed, so the
// sessions of the others go on.
func (d *Door) SetPeers(peers []Peer) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	want := map[Key]netip.Addr{}
	for _, p := range peers {
		want[p.PublicKey] = p.Address
	}
	var b strings.Builder
	for k, a := range d.peers {
		if w, ok := want[k]; !ok || w != a {
			fmt.Fprintf(&b, "public_key=%s\nremove=true\n", k.Hex())
		}
	}
	for k, a := range want {
		if have, ok := d.peers[k]; !ok || have != a {
			fmt.Fprintf(&b, "public_key=%s\nreplace_allowed_ips=true\nallowed_ip=%s/32\n", k.Hex(), a)
		}
	}
	if b.Len() == 0 {
		return nil
	}
	if err := d.dev.IpcSet(b.String()); err != nil {
		return err
	}
	d.peers = want
	return nil
}

// Port is the UDP port the door listens on (the one asked for, or the one the system chose
// for 0).
func (d *Door) Port() int {
	conf, err := d.dev.IpcGet()
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(conf, "\n") {
		if v, ok := strings.CutPrefix(line, "listen_port="); ok {
			n, _ := strconv.Atoi(v)
			return n
		}
	}
	return 0
}

// PeerStatus is a peer as WireGuard sees it.
type PeerStatus struct {
	Endpoint      string // IP:port of the peer's last packet; "" before one
	LastHandshake time.Time
}

// PeerStatus is every peer's endpoint and last handshake.
func (d *Door) PeerStatus() map[Key]PeerStatus {
	conf, err := d.dev.IpcGet()
	if err != nil {
		return nil
	}
	return parsePeerStatus(conf)
}

func parsePeerStatus(conf string) map[Key]PeerStatus {
	out := map[Key]PeerStatus{}
	var cur *Key
	var st PeerStatus
	var sec, nsec int64
	flush := func() {
		if cur != nil {
			if sec != 0 || nsec != 0 {
				st.LastHandshake = time.Unix(sec, nsec)
			}
			out[*cur] = st
		}
	}
	for _, line := range strings.Split(conf, "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch k {
		case "public_key":
			flush()
			key, err := parseHexKey(v)
			if err != nil {
				cur = nil
				continue
			}
			cur, st, sec, nsec = &key, PeerStatus{}, 0, 0
		case "endpoint":
			st.Endpoint = v
		case "last_handshake_time_sec":
			sec, _ = strconv.ParseInt(v, 10, 64)
		case "last_handshake_time_nsec":
			nsec, _ = strconv.ParseInt(v, 10, 64)
		}
	}
	flush()
	return out
}

// Close takes the door down.
func (d *Door) Close() { d.dev.Close() }

// Client is a worker's end: its own stack, one peer (the queue), and an HTTP client whose
// requests go only through the tunnel.
type Client struct {
	dev  *device.Device
	net  *netstack.Net
	HTTP *http.Client
}

// Dial brings the worker's tunnel up. endpoint overrides the configuration's endpoint when it
// is not empty (a worker beside the queue reaches it on another address).
func Dial(priv Key, cfg Config, endpoint string, log *slog.Logger) (*Client, error) {
	if endpoint == "" {
		endpoint = cfg.Endpoint
	}
	ep, err := resolveUDP(endpoint)
	if err != nil {
		return nil, fmt.Errorf("endpoint %s: %w", endpoint, err)
	}
	tdev, tnet, err := netstack.CreateNetTUN([]netip.Addr{cfg.Address.Addr()}, nil, MTU)
	if err != nil {
		return nil, err
	}
	dev := device.NewDevice(tdev, conn.NewDefaultBind(), logger(log, "wireguard: "))
	conf := fmt.Sprintf("private_key=%s\npublic_key=%s\nendpoint=%s\nallowed_ip=%s\npersistent_keepalive_interval=%d\n",
		priv.Hex(), cfg.ServerPublicKey.Hex(), ep, cfg.AllowedIPs, cfg.Keepalive)
	if err := dev.IpcSet(conf); err != nil {
		dev.Close()
		return nil, err
	}
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, err
	}
	tr := &http.Transport{
		DialContext:           tnet.DialContext,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
	return &Client{dev: dev, net: tnet, HTTP: &http.Client{Transport: tr}}, nil
}

// Close takes the tunnel down.
func (c *Client) Close() { c.dev.Close() }

func resolveUDP(hostport string) (netip.AddrPort, error) {
	if ap, err := netip.ParseAddrPort(hostport); err == nil {
		return ap, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return netip.AddrPort{}, err
	}
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return netip.AddrPort{}, err
	}
	if len(addrs) == 0 {
		return netip.AddrPort{}, fmt.Errorf("no address for %s", host)
	}
	p, err := net.LookupPort("udp", port)
	if err != nil {
		return netip.AddrPort{}, err
	}
	// Prefer IPv4: wireguard-go's default bind listens on both, and v4 is what home routers forward.
	best := addrs[0]
	for _, a := range addrs {
		if a.Is4() || a.Is4In6() {
			best = a.Unmap()
			break
		}
	}
	return netip.AddrPortFrom(best, uint16(p)), nil
}
