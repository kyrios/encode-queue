// Package tunnel is the WireGuard of both ends: wireguard-go in userspace with its own network
// stack (gVisor), so neither the queue nor a worker needs a host interface or network rights.
package tunnel

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/curve25519"
)

// Key is a WireGuard key, private or public.
type Key [32]byte

// GenerateKey makes a new private key, clamped as Curve25519 wants it.
func GenerateKey() (Key, error) {
	var k Key
	if _, err := rand.Read(k[:]); err != nil {
		return k, err
	}
	k[0] &= 248
	k[31] = (k[31] & 127) | 64
	return k, nil
}

// Public is the public key of a private key.
func (k Key) Public() Key {
	var pub Key
	out, err := curve25519.X25519(k[:], curve25519.Basepoint)
	if err != nil {
		panic(err) // only for an all-zero result, which a clamped key never gives
	}
	copy(pub[:], out)
	return pub
}

// String is the key in base64, as `wg` prints it.
func (k Key) String() string { return base64.StdEncoding.EncodeToString(k[:]) }

// Hex is the key as wireguard-go's configuration protocol wants it.
func (k Key) Hex() string { return hex.EncodeToString(k[:]) }

// ParseKey reads a key in base64.
func ParseKey(s string) (Key, error) {
	var k Key
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil || len(b) != 32 {
		return k, errors.New("not a WireGuard key: want 32 bytes in base64")
	}
	copy(k[:], b)
	return k, nil
}

// LoadOrCreateKey reads the private key in path, or makes one there (mode 0600) when the file
// does not exist yet.
func LoadOrCreateKey(path string) (Key, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		return ParseKey(string(data))
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Key{}, err
	}
	k, err := GenerateKey()
	if err != nil {
		return k, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return k, err
	}
	tmp := path + ".new"
	if err := os.WriteFile(tmp, []byte(k.String()+"\n"), 0o600); err != nil {
		return k, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return k, fmt.Errorf("keep the key: %w", err)
	}
	return k, nil
}
