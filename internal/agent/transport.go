package agent

import (
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"time"
)

// transport is the encrypted agent-to-agent channel, for shipping checkpoint images over a network
// that isn't encrypted already (a LAN, rather than the WireGuard tailnet the node IPs are on).
//
// Every agent derives the same ed25519 key from the agent token (HKDF-SHA256, label
// transportLabel) and wraps it in a self-signed certificate. Both ends of a connection present it
// and accept only a peer presenting that exact public key: TLS 1.3 with mutual authentication by
// key pinning, no CA and nothing to distribute or renew. Holding the agent token already means
// full control of every agent, so this adds no new trust: someone on the network without the
// token can neither read nor alter the stream, nor pose as either end. Requests still carry the
// bearer token, so the TLS listener serves the same handler with the same authorization.
type transport struct {
	cert tls.Certificate
	pub  ed25519.PublicKey
}

const transportLabel = "lcm-transport-v1"

func newTransport(token string) *transport {
	seed, err := hkdf.Key(sha256.New, []byte(token), nil, transportLabel, ed25519.SeedSize)
	if err != nil {
		panic(err) // only for lengths SHA-256 can't produce
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	// Validity and names are never checked (see verify); the dates only have to parse.
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "lcm-agent"},
		NotBefore:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:     time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		panic(err) // can't fail for this template and key
	}
	return &transport{cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}, pub: pub}
}

// verify accepts a peer whose certificate carries the token-derived key, and nothing else. TLS
// 1.3 has already checked that the peer holds the private key (CertificateVerify, with distinct
// client and server contexts, so one end's signature can't be reflected back as the other's).
func (t *transport) verify(cs tls.ConnectionState) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("lcm transport: peer presented no certificate")
	}
	if pub, ok := cs.PeerCertificates[0].PublicKey.(ed25519.PublicKey); !ok || !pub.Equal(t.pub) {
		return errors.New("lcm transport: peer key doesn't match this agent token")
	}
	return nil
}

// HTTP/1.1 only: one big upload per connection, and HTTP/2's flow-control windows would only
// throttle it.
func (t *transport) serverConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{t.cert},
		ClientAuth: tls.RequireAnyClientCert, VerifyConnection: t.verify, NextProtos: []string{"http/1.1"}}
}

func (t *transport) clientConfig() *tls.Config {
	// The server is checked by its key in verify, not by name or chain.
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{t.cert},
		InsecureSkipVerify: true, VerifyConnection: t.verify, NextProtos: []string{"http/1.1"}}
}

// TLSListener wraps ln in the encrypted transport and advertises its port in GET /v1/info
// ("tlsPort"), where the lcm CLI looks before shipping over a --transfer-network.
func (a *Agent) TLSListener(ln net.Listener) net.Listener {
	if ta, ok := ln.Addr().(*net.TCPAddr); ok {
		a.tlsPort.Store(int32(ta.Port))
	}
	return tls.NewListener(ln, a.tr.serverConfig())
}
