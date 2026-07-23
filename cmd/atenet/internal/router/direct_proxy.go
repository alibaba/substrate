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
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func (s *RouterServer) serveDirectHTTPProxy(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.HttpPort)
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

	target := &url.URL{
		Scheme: "http",
		Host:   routeTarget.HostPort(),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(out *http.Request) {
		originalDirector(out)
		out.Host = target.Host
		out.URL.Scheme = target.Scheme
		out.URL.Host = target.Host
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		slog.ErrorContext(req.Context(), "direct HTTP proxy failed",
			slog.String("atespace", atespace),
			slog.String("actor", actorName),
			slog.String("target", target.Host),
			slog.Any("err", err))
		http.Error(w, "upstream actor request failed", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, req)
}

func writeReqError(w http.ResponseWriter, err error) {
	var reqErr *reqError
	if errors.As(err, &reqErr) {
		http.Error(w, reqErr.Error(), reqErr.statusCode)
		return
	}
	http.Error(w, "internal router error", http.StatusInternalServerError)
}
