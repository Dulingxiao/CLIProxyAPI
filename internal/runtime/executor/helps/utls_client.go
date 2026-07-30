package helps

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	tls "github.com/refraction-networking/utls"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
)

// utlsRoundTripper implements http.RoundTripper using utls with Chrome fingerprint
// to bypass Cloudflare's TLS fingerprinting on Anthropic domains.
type utlsRoundTripper struct {
	mu          sync.Mutex
	connections map[string]*http2.ClientConn
	pending     map[string]*sync.Cond
	dialer      proxy.Dialer
}

func newUtlsRoundTripper(proxyURL string) *utlsRoundTripper {
	var dialer proxy.Dialer = proxy.Direct
	if proxyURL != "" {
		proxyDialer, mode, errBuild := proxyutil.BuildDialer(proxyURL)
		if errBuild != nil {
			log.Errorf("utls: failed to configure proxy dialer for %q: %v", proxyutil.Redact(proxyURL), errBuild)
		} else if mode != proxyutil.ModeInherit && proxyDialer != nil {
			dialer = proxyDialer
		}
	}
	return &utlsRoundTripper{
		connections: make(map[string]*http2.ClientConn),
		pending:     make(map[string]*sync.Cond),
		dialer:      dialer,
	}
}

func (t *utlsRoundTripper) getOrCreateConnection(host, addr string) (*http2.ClientConn, error) {
	t.mu.Lock()

	if h2Conn, ok := t.connections[host]; ok && h2Conn.CanTakeNewRequest() {
		t.mu.Unlock()
		return h2Conn, nil
	}

	if cond, ok := t.pending[host]; ok {
		cond.Wait()
		if h2Conn, ok := t.connections[host]; ok && h2Conn.CanTakeNewRequest() {
			t.mu.Unlock()
			return h2Conn, nil
		}
	}

	cond := sync.NewCond(&t.mu)
	t.pending[host] = cond
	t.mu.Unlock()

	h2Conn, err := t.createConnection(host, addr)

	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.pending, host)
	cond.Broadcast()

	if err != nil {
		return nil, err
	}

	t.connections[host] = h2Conn
	return h2Conn, nil
}

func (t *utlsRoundTripper) createConnection(host, addr string) (*http2.ClientConn, error) {
	conn, err := t.dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{ServerName: host}
	tlsConn := tls.UClient(conn, tlsConfig, tls.HelloChrome_Auto)

	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		return nil, err
	}

	tr := &http2.Transport{}
	h2Conn, err := tr.NewClientConn(tlsConn)
	if err != nil {
		tlsConn.Close()
		return nil, err
	}

	return h2Conn, nil
}

func (t *utlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	hostname := req.URL.Hostname()
	port := req.URL.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(hostname, port)

	h2Conn, err := t.getOrCreateConnection(hostname, addr)
	if err != nil {
		return nil, err
	}

	resp, err := h2Conn.RoundTrip(req)
	if err != nil {
		t.mu.Lock()
		if cached, ok := t.connections[hostname]; ok && cached == h2Conn {
			delete(t.connections, hostname)
		}
		t.mu.Unlock()
		return nil, err
	}

	return resp, nil
}

// utlsProtectedHosts contains the hosts that should use utls Chrome TLS fingerprint
// to bypass Cloudflare's TLS fingerprinting.
var utlsProtectedHosts = map[string]struct{}{
	"api.anthropic.com": {},
	"chatgpt.com":       {},
}

// fallbackRoundTripper uses utls for protected HTTPS hosts and falls back to
// standard transport for all other requests.
type fallbackRoundTripper struct {
	utls     http.RoundTripper
	fallback http.RoundTripper
}

func (f *fallbackRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		if _, ok := utlsProtectedHosts[strings.ToLower(req.URL.Hostname())]; ok {
			return f.utls.RoundTrip(req)
		}
	}
	return f.fallback.RoundTrip(req)
}

// NewUtlsHTTPClient creates an HTTP client using utls Chrome TLS fingerprint.
// Use this for provider requests that need a Chrome-like TLS fingerprint.
// Falls back to standard transport for non-HTTPS requests.
func NewUtlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	var proxyURL string
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}

	var ctxRoundTripper http.RoundTripper
	if ctx != nil {
		ctxRoundTripper, _ = ctx.Value("cliproxy.roundtripper").(http.RoundTripper)
	}

	var utlsRT http.RoundTripper = newUtlsRoundTripper(proxyURL)
	var standardTransport http.RoundTripper = http.DefaultTransport
	if proxyURL != "" {
		if transport := buildProxyTransport(proxyURL); transport != nil {
			standardTransport = transport
		}
	} else if ctxRoundTripper != nil {
		utlsRT = ctxRoundTripper
		standardTransport = ctxRoundTripper
	}

	client := &http.Client{
		Transport: &fallbackRoundTripper{
			utls:     utlsRT,
			fallback: standardTransport,
		},
	}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}

// NewUTLSWebsocketDialContext creates a TLS dial function that uses a Chrome
// ClientHello while keeping WebSocket traffic on HTTP/1.1.
func NewUTLSWebsocketDialContext(proxyURL string) (func(context.Context, string, string) (net.Conn, error), error) {
	return newUTLSWebsocketDialContext(proxyURL, nil)
}

func newUTLSWebsocketDialContext(proxyURL string, tlsConfig *tls.Config) (func(context.Context, string, string) (net.Conn, error), error) {
	baseDialer, mode, errBuild := proxyutil.BuildDialer(proxyURL)
	if errBuild != nil {
		return nil, fmt.Errorf("build websocket proxy dialer: %w", errBuild)
	}
	if mode == proxyutil.ModeInherit || baseDialer == nil {
		baseDialer = proxy.Direct
	}
	contextDialer, ok := baseDialer.(proxy.ContextDialer)
	if !ok {
		return nil, fmt.Errorf("websocket proxy dialer does not support context cancellation")
	}

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if ctx == nil {
			ctx = context.Background()
		}
		rawConn, errDial := contextDialer.DialContext(ctx, network, addr)
		if errDial != nil {
			return nil, errDial
		}

		configForConn := &tls.Config{}
		if tlsConfig != nil {
			configForConn = tlsConfig.Clone()
		}
		host, _, errSplit := net.SplitHostPort(addr)
		if errSplit != nil {
			host = addr
		}
		configForConn.ServerName = host

		spec, errSpec := codexChromeWebsocketClientHelloSpec()
		if errSpec != nil {
			_ = rawConn.Close()
			return nil, fmt.Errorf("build Chrome websocket ClientHello: %w", errSpec)
		}
		tlsConn := tls.UClient(rawConn, configForConn, tls.HelloCustom)
		if errPreset := tlsConn.ApplyPreset(&spec); errPreset != nil {
			_ = rawConn.Close()
			return nil, fmt.Errorf("apply Chrome websocket ClientHello: %w", errPreset)
		}
		if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
			_ = rawConn.Close()
			return nil, fmt.Errorf("Chrome websocket TLS handshake: %w", errHandshake)
		}
		return tlsConn, nil
	}, nil
}

func codexChromeWebsocketClientHelloSpec() (tls.ClientHelloSpec, error) {
	spec, errSpec := tls.UTLSIdToSpec(tls.HelloChrome_Auto)
	if errSpec != nil {
		return tls.ClientHelloSpec{}, errSpec
	}

	extensions := make([]tls.TLSExtension, 0, len(spec.Extensions))
	for _, extension := range spec.Extensions {
		switch typed := extension.(type) {
		case *tls.ALPNExtension:
			typed.AlpnProtocols = []string{"http/1.1"}
			extensions = append(extensions, typed)
		case *tls.ApplicationSettingsExtension, *tls.ApplicationSettingsExtensionNew:
			// ALPS is specific to HTTP/2 or HTTP/3 and conflicts with an HTTP/1.1 upgrade.
			continue
		default:
			extensions = append(extensions, extension)
		}
	}
	spec.Extensions = extensions
	return spec, nil
}
