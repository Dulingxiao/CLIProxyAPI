package helps

import (
	"bufio"
	"context"
	stdtls "crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	utls "github.com/refraction-networking/utls"
)

type utlsClientRoundTripFunc func(*http.Request) (*http.Response, error)

func (f utlsClientRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewUtlsHTTPClientUsesContextRoundTripperForProtectedHost(t *testing.T) {
	t.Parallel()

	called := false
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", utlsClientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		if req.URL.Hostname() != "chatgpt.com" {
			t.Fatalf("hostname = %q, want chatgpt.com", req.URL.Hostname())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("{}")),
			Request:    req,
		}, nil
	}))

	client := NewUtlsHTTPClient(ctx, nil, nil, 0)
	resp, err := client.Get("https://chatgpt.com/backend-api/codex/responses")
	if err != nil {
		t.Fatalf("client.Get returned error: %v", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		t.Fatalf("response body close returned error: %v", errClose)
	}
	if !called {
		t.Fatal("expected context RoundTripper to handle protected host request")
	}
}

func TestCodexChromeWebsocketSpecUsesHTTP11(t *testing.T) {
	t.Parallel()

	spec, err := codexChromeWebsocketClientHelloSpec()
	if err != nil {
		t.Fatalf("codexChromeWebsocketClientHelloSpec returned error: %v", err)
	}

	var alpn []string
	for _, extension := range spec.Extensions {
		switch typed := extension.(type) {
		case *utls.ALPNExtension:
			alpn = typed.AlpnProtocols
		case *utls.ApplicationSettingsExtension, *utls.ApplicationSettingsExtensionNew:
			t.Fatalf("websocket ClientHello retained HTTP/2 application settings: %T", typed)
		}
	}
	if got, want := strings.Join(alpn, ","), "http/1.1"; got != want {
		t.Fatalf("ALPN = %q, want %q", got, want)
	}
}

func TestUTLSWebsocketDialContextNegotiatesHTTP11(t *testing.T) {
	t.Parallel()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.EnableHTTP2 = true
	server.TLS = &stdtls.Config{NextProtos: []string{"h2", "http/1.1"}}
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()

	dialTLSContext, err := newUTLSWebsocketDialContext("direct", &utls.Config{InsecureSkipVerify: true}) //nolint:gosec -- local test server
	if err != nil {
		t.Fatalf("newUTLSWebsocketDialContext returned error: %v", err)
	}
	conn, err := dialTLSContext(context.Background(), "tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dialTLSContext returned error: %v", err)
	}
	defer conn.Close()

	utlsConn, ok := conn.(*utls.UConn)
	if !ok {
		t.Fatalf("connection type = %T, want *utls.UConn", conn)
	}
	if got, want := utlsConn.ConnectionState().NegotiatedProtocol, "http/1.1"; got != want {
		t.Fatalf("negotiated protocol = %q, want %q", got, want)
	}
}

func TestUTLSWebsocketDialContextTunnelsThroughHTTPProxy(t *testing.T) {
	t.Parallel()

	target := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	target.TLS = &stdtls.Config{NextProtos: []string{"http/1.1"}}
	target.Config.ErrorLog = log.New(io.Discard, "", 0)
	target.StartTLS()
	defer target.Close()

	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen returned error: %v", err)
	}
	defer proxyListener.Close()

	proxyDone := make(chan error, 1)
	go func() {
		clientConn, errAccept := proxyListener.Accept()
		if errAccept != nil {
			proxyDone <- errAccept
			return
		}
		defer clientConn.Close()

		clientReader := bufio.NewReader(clientConn)
		req, errRead := http.ReadRequest(clientReader)
		if errRead != nil {
			proxyDone <- fmt.Errorf("read CONNECT request: %w", errRead)
			return
		}
		if req.Method != http.MethodConnect || req.Host != target.Listener.Addr().String() {
			proxyDone <- fmt.Errorf("CONNECT target = %s %s", req.Method, req.Host)
			return
		}

		upstreamConn, errDial := net.Dial("tcp", target.Listener.Addr().String())
		if errDial != nil {
			proxyDone <- errDial
			return
		}
		defer upstreamConn.Close()
		if _, errWrite := io.WriteString(clientConn, "HTTP/1.1 200 Connection Established\r\n\r\n"); errWrite != nil {
			proxyDone <- errWrite
			return
		}

		copyDone := make(chan struct{}, 2)
		go func() {
			_, _ = io.Copy(upstreamConn, clientReader)
			copyDone <- struct{}{}
		}()
		go func() {
			_, _ = io.Copy(clientConn, upstreamConn)
			copyDone <- struct{}{}
		}()
		<-copyDone
		proxyDone <- nil
	}()

	dialTLSContext, err := newUTLSWebsocketDialContext(
		"http://"+proxyListener.Addr().String(),
		&utls.Config{InsecureSkipVerify: true}, //nolint:gosec -- local test server
	)
	if err != nil {
		t.Fatalf("newUTLSWebsocketDialContext returned error: %v", err)
	}
	conn, err := dialTLSContext(context.Background(), "tcp", target.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dialTLSContext returned error: %v", err)
	}
	if _, ok := conn.(*utls.UConn); !ok {
		t.Fatalf("connection type = %T, want *utls.UConn", conn)
	}
	if errClose := conn.Close(); errClose != nil {
		t.Fatalf("conn.Close returned error: %v", errClose)
	}

	select {
	case errProxy := <-proxyDone:
		if errProxy != nil {
			t.Fatalf("proxy returned error: %v", errProxy)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not finish after tunneled TLS connection closed")
	}
}
