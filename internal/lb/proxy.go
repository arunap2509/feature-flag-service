package lb

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"feature-flag-service/internal/constants"
)

type contextKey string

const targetContextKey contextKey = "upstream_target"

// ProxyServer encapsulates the dynamic reverse proxy routing incoming client traffic
type ProxyServer struct {
	pool  *UpstreamPool
	proxy *httputil.ReverseProxy
}

// NewProxyServer initializes a reverse proxy with immediate SSE flushing (-1)
func NewProxyServer(pool *UpstreamPool) *ProxyServer {
	director := func(req *http.Request) {
		target, ok := req.Context().Value(targetContextKey).(*url.URL)
		if !ok || target == nil {
			return
		}

		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host

		// Append or initialize X-Forwarded-For
		if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
			if prior, exists := req.Header[constants.HeaderXForwardedFor]; exists {
				clientIP = strings.Join(prior, ", ") + ", " + clientIP
			}
			req.Header.Set(constants.HeaderXForwardedFor, clientIP)
		}
	}

	errorHandler := func(w http.ResponseWriter, req *http.Request, err error) {
		log.Printf("[%s] Upstream proxy error for %s %s: %v",
			constants.ServiceNameLoadBalancer, req.Method, req.URL.Path, err)
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   constants.ErrMsgUpstreamConnectionError,
			"details": err.Error(),
		})
	}

	proxy := &httputil.ReverseProxy{
		Director:      director,
		FlushInterval: -1, // CRITICAL: -1 forces immediate flushing for real-time SSE stream events
		ErrorHandler:  errorHandler,
	}

	return &ProxyServer{
		pool:  pool,
		proxy: proxy,
	}
}

// ServeHTTP handles /healthz or dynamically load balances client requests to healthy Connection Servers
func (ps *ProxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == constants.HealthzPath {
		ps.handleHealthz(w, r)
		return
	}

	target, err := ps.pool.Next()
	if err != nil {
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": constants.ErrMsgNoUpstreamsAvailable,
		})
		return
	}

	ctx := context.WithValue(r.Context(), targetContextKey, target)
	ps.proxy.ServeHTTP(w, r.WithContext(ctx))
}

func (ps *ProxyServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	targets := ps.pool.Targets()
	status := constants.StatusHealthy
	statusCode := http.StatusOK

	if len(targets) == 0 {
		status = constants.StatusDegraded
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          status,
		"upstreams_count": len(targets),
		"upstreams":       targets,
	})
}
