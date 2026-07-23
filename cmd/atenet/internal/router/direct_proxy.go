// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	directProxyRetryBudget = 12 * time.Second
	directProxyRetryDelay  = 50 * time.Millisecond
	directProxyStaleHeader = "X-Substrate-Stale"

	directProxyMigrationAttemptTimeout = 250 * time.Millisecond
)

type cachedDirectProxyResponse struct {
	statusCode int
	header     http.Header
	body       []byte
}

type directProxyResponseCache struct {
	mu      sync.RWMutex
	entries map[string]cachedDirectProxyResponse
}

func (s *RouterServer) serveDirectHTTPProxy(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.directHTTPProxyPort())
	server := &http.Server{
		Addr:    addr,
		Handler: http.HandlerFunc(s.handleDirectProxy),
	}

	errCh := make(chan error, 1)
	go func() {
		slog.InfoContext(ctx, "Starting direct HTTP proxy", slog.String("addr", addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		return server.Shutdown(context.Background())
	case err := <-errCh:
		return err
	}
}

func (s *RouterServer) directHTTPProxyPort() int {
	if s.cfg.DirectHTTPProxyPort > 0 {
		return s.cfg.DirectHTTPProxyPort
	}
	return s.cfg.HttpPort
}

func (s *RouterServer) handleDirectProxy(w http.ResponseWriter, req *http.Request) {
	atespace, actorName, err := parseActorRef(req.Host)
	if err != nil {
		http.Error(w, invalidHostErr(req.Host, err).Error(), http.StatusNotFound)
		return
	}

	actorKey := atespace + "/" + actorName
	done := s.inflight.begin(actorKey)
	defer done()

	_, routeTarget, err := s.extprocSrv.routeResolver.Resolve(req.Context(), atespace, actorName)
	if err != nil {
		writeReqError(w, mapRouteError(actorName, err))
		return
	}

	cacheKey := directProxyCacheKey(atespace, actorName, req)
	if directProxyStaleCacheEnabled() && shouldServeCachedDirectProxyResponseBeforeUpstream(routeTarget.Phase) && s.writeCachedDirectProxyResponse(w, cacheKey) {
		return
	}

	if err := s.serveDirectProxyTarget(w, req, atespace, actorName, routeTarget); err == nil {
		return
	} else if !isDirectProxyRetryable(req) {
		slog.ErrorContext(req.Context(), "direct HTTP proxy failed",
			slog.String("atespace", atespace),
			slog.String("actor", actorName),
			slog.String("target", routeTarget.HostPort()),
			slog.Any("err", err))
		http.Error(w, "upstream actor request failed", http.StatusBadGateway)
		return
	} else if directProxyStaleCacheEnabled() && s.writeCachedDirectProxyResponse(w, cacheKey) {
		return
	}

	deadline := time.Now().Add(directProxyRetryBudget)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-req.Context().Done():
			return
		case <-time.After(directProxyRetryDelay):
		}

		_, routeTarget, err = s.extprocSrv.routeResolver.Resolve(req.Context(), atespace, actorName)
		if err != nil {
			lastErr = err
			continue
		}
		if err := s.serveDirectProxyTarget(w, req, atespace, actorName, routeTarget); err != nil {
			lastErr = err
			if directProxyStaleCacheEnabled() && s.writeCachedDirectProxyResponse(w, cacheKey) {
				return
			}
			continue
		}
		return
	}

	slog.ErrorContext(req.Context(), "direct HTTP proxy failed after retry",
		slog.String("atespace", atespace),
		slog.String("actor", actorName),
		slog.String("target", routeTarget.HostPort()),
		slog.Any("err", lastErr))
	http.Error(w, "upstream actor request failed", http.StatusBadGateway)
}

func (s *RouterServer) serveDirectProxyTarget(w http.ResponseWriter, req *http.Request, atespace, actorName string, routeTarget routeTarget) error {
	cacheKey := directProxyCacheKey(atespace, actorName, req)
	targetHost := routeTarget.HostPort()
	out := req.Clone(req.Context())
	out.URL.Scheme = "http"
	out.URL.Host = targetHost
	out.Host = targetHost
	out.RequestURI = ""
	if shouldBoundDirectProxyAttempt(routeTarget.Phase) || (directProxyStaleCacheEnabled() && s.hasCachedDirectProxyResponse(cacheKey, req)) {
		attemptCtx, cancel := context.WithTimeout(req.Context(), directProxyMigrationAttemptTimeout)
		defer cancel()
		out = out.WithContext(attemptCtx)
	}

	transport := s.directProxyTransport
	if transport == nil {
		transport = http.DefaultTransport
	}

	resp, err := transport.RoundTrip(out)
	if err != nil {
		slog.WarnContext(req.Context(), "direct HTTP proxy upstream attempt failed",
			slog.String("atespace", atespace),
			slog.String("actor", actorName),
			slog.String("target", targetHost),
			slog.Any("err", err))
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	s.storeCachedDirectProxyResponse(cacheKey, req, resp.StatusCode, resp.Header, body)
	return nil
}

func directProxyStaleCacheEnabled() bool {
	return os.Getenv("ATENET_DIRECT_PROXY_STALE_CACHE") == "1"
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func directProxyCacheKey(atespace, actorName string, req *http.Request) string {
	return atespace + "/" + actorName + " " + req.Method + " " + req.URL.RequestURI()
}

func (s *RouterServer) storeCachedDirectProxyResponse(key string, req *http.Request, statusCode int, header http.Header, body []byte) {
	if req.Method != http.MethodGet || statusCode != http.StatusOK {
		return
	}
	s.directProxyCache.mu.Lock()
	defer s.directProxyCache.mu.Unlock()
	if s.directProxyCache.entries == nil {
		s.directProxyCache.entries = map[string]cachedDirectProxyResponse{}
	}
	s.directProxyCache.entries[key] = cachedDirectProxyResponse{
		statusCode: statusCode,
		header:     header.Clone(),
		body:       append([]byte(nil), body...),
	}
}

func (s *RouterServer) hasCachedDirectProxyResponse(key string, req *http.Request) bool {
	if req.Method != http.MethodGet {
		return false
	}
	s.directProxyCache.mu.RLock()
	_, ok := s.directProxyCache.entries[key]
	s.directProxyCache.mu.RUnlock()
	return ok
}

func (s *RouterServer) writeCachedDirectProxyResponse(w http.ResponseWriter, key string) bool {
	s.directProxyCache.mu.RLock()
	cached, ok := s.directProxyCache.entries[key]
	s.directProxyCache.mu.RUnlock()
	if !ok {
		return false
	}
	copyHeader(w.Header(), cached.header)
	w.Header().Set(directProxyStaleHeader, "true")
	w.WriteHeader(cached.statusCode)
	if len(cached.body) > 0 {
		_, _ = w.Write(cached.body)
	}
	return true
}

func shouldServeCachedDirectProxyResponseBeforeUpstream(phase ateapipb.ActorRoute_Phase) bool {
	switch phase {
	case ateapipb.ActorRoute_PHASE_DRAINING, ateapipb.ActorRoute_PHASE_SWITCHED:
		return true
	default:
		return false
	}
}

func shouldBoundDirectProxyAttempt(phase ateapipb.ActorRoute_Phase) bool {
	switch phase {
	case ateapipb.ActorRoute_PHASE_DRAINING, ateapipb.ActorRoute_PHASE_SWITCHED:
		return true
	default:
		return false
	}
}

func isDirectProxyRetryable(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func writeReqError(w http.ResponseWriter, err error) {
	var reqErr *reqError
	if errors.As(err, &reqErr) {
		http.Error(w, reqErr.Error(), reqErr.statusCode)
		return
	}
	http.Error(w, "internal router error", http.StatusInternalServerError)
}
