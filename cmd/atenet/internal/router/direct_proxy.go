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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/gorilla/websocket"
)

const (
	directProxyRetryBudget             = 60 * time.Second
	directProxyRetryDelay              = 50 * time.Millisecond
	directProxyStaleHeader             = "X-Substrate-Stale"
	directProxyBoundAttemptHeader      = "X-Substrate-Bound-Attempt"
	directProxyRouteGenerationHeader   = "X-Substrate-Route-Generation"
	directProxyRoutePhaseHeader        = "X-Substrate-Route-Phase"
	directProxyRouteTargetWorkerHeader = "X-Substrate-Route-Target-Worker"
	directProxyRouteTargetNodeHeader   = "X-Substrate-Route-Target-Node"
	directProxyRouteTargetIPHeader     = "X-Substrate-Route-Target-IP"

	directProxyMigrationAttemptTimeout = 250 * time.Millisecond
	directProxySSEIdleTimeout          = 750 * time.Millisecond
	directProxyWebSocketIdleTimeout    = 750 * time.Millisecond
	directProxyMaxUpstreamPerActor     = 4
	directProxyMaxStreamPerActor       = 2
	directProxyMaxConnsPerHost         = 32
	directProxyMaxReplayBodyBytes      = 32 << 20
)

var directProxyDefaultTransport http.RoundTripper = newDirectProxyDefaultTransport()

var errDirectProxyReplayBodyTooLarge = errors.New("retryable direct proxy request body is too large to replay")

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
	if isDirectProxyUpgradeRequest(req) {
		if err := s.proxyDirectUpgrade(w, req, atespace, actorName, routeTarget); err != nil {
			slog.ErrorContext(req.Context(), "direct HTTP proxy upgrade failed",
				slog.String("atespace", atespace),
				slog.String("actor", actorName),
				slog.String("target", routeTarget.HostPort()),
				slog.Any("err", err))
		}
		return
	}

	cacheKey := directProxyCacheKey(atespace, actorName, req)
	if err := prepareDirectProxyReplayBody(req); err != nil {
		if errors.Is(err, errDirectProxyReplayBodyTooLarge) {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read retryable request body", http.StatusBadRequest)
		return
	}
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

	deadline := time.Now().Add(directProxyRetryBudgetValue())
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-req.Context().Done():
			return
		case <-time.After(directProxyRetryDelayValue()):
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
	actorKey := atespace + "/" + actorName
	resp, cancelUpstream, err := s.openDirectProxyUpstream(req, actorKey, routeTarget, shouldBoundDirectProxyAttempt(req, routeTarget.Phase, directProxyStaleCacheEnabled() && s.hasCachedDirectProxyResponse(cacheKey, req)))
	if err != nil {
		slog.WarnContext(req.Context(), "direct HTTP proxy upstream attempt failed",
			slog.String("atespace", atespace),
			slog.String("actor", actorName),
			slog.String("target", routeTarget.HostPort()),
			slog.Any("err", err))
		return err
	}
	defer cancelUpstream()
	defer resp.Body.Close()

	if shouldStreamDirectProxyResponse(resp) {
		return s.streamDirectProxyResponse(w, req, atespace, actorName, resp, cancelUpstream, routeTarget)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	copyHeader(w.Header(), resp.Header)
	writeRouteTraceHeaders(w.Header(), routeTarget)
	w.WriteHeader(resp.StatusCode)
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	s.storeCachedDirectProxyResponse(cacheKey, req, resp.StatusCode, resp.Header, body)
	return nil
}

func (s *RouterServer) proxyDirectUpgrade(w http.ResponseWriter, req *http.Request, atespace, actorName string, routeTarget routeTarget) error {
	if isDirectProxyTerminalWebSocket(req) {
		return s.proxyDirectTerminalWebSocket(w, req, atespace, actorName, routeTarget)
	}
	return s.proxyDirectRawUpgrade(w, req, routeTarget.HostPort(), routeTarget)
}

func (s *RouterServer) proxyDirectRawUpgrade(w http.ResponseWriter, req *http.Request, targetAddr string, routeTarget routeTarget) error {
	upstream, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
	if err != nil {
		http.Error(w, "upstream actor upgrade failed", http.StatusBadGateway)
		return err
	}
	defer upstream.Close()

	upstreamReq := req.Clone(req.Context())
	upstreamReq.URL.Scheme = "http"
	upstreamReq.URL.Host = targetAddr
	upstreamReq.Host = targetAddr
	upstreamReq.RequestURI = ""
	if err := upstreamReq.Write(upstream); err != nil {
		http.Error(w, "upstream actor upgrade failed", http.StatusBadGateway)
		return err
	}

	upstreamReader := bufio.NewReader(upstream)
	resp, err := http.ReadResponse(upstreamReader, upstreamReq)
	if err != nil {
		http.Error(w, "upstream actor upgrade failed", http.StatusBadGateway)
		return err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		defer resp.Body.Close()
		copyHeader(w.Header(), resp.Header)
		writeRouteTraceHeaders(w.Header(), routeTarget)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return fmt.Errorf("upstream upgrade status=%d", resp.StatusCode)
	}
	writeRouteTraceHeaders(resp.Header, routeTarget)

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return fmt.Errorf("response writer does not support hijacking")
	}
	downstream, downstreamBuf, err := hijacker.Hijack()
	if err != nil {
		return err
	}
	defer downstream.Close()
	if buffered := downstreamBuf.Reader.Buffered(); buffered > 0 {
		buf := make([]byte, buffered)
		if _, err := io.ReadFull(downstreamBuf.Reader, buf); err != nil {
			return err
		}
		if _, err := upstream.Write(buf); err != nil {
			return err
		}
	}
	if err := resp.Write(downstream); err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go func() {
		_, copyErr := io.Copy(upstream, downstream)
		errCh <- copyErr
	}()
	go func() {
		_, copyErr := io.Copy(downstream, upstreamReader)
		errCh <- copyErr
	}()
	select {
	case <-req.Context().Done():
		return nil
	case err := <-errCh:
		if err == nil || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
}

var directProxyWebSocketUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

type directProxyWSMessage struct {
	messageType int
	payload     []byte
}

type directProxyTerminalState struct {
	pending  map[int]directProxyWSMessage
	lastTick int64
}

func (s *RouterServer) proxyDirectTerminalWebSocket(w http.ResponseWriter, req *http.Request, atespace, actorName string, routeTarget routeTarget) error {
	responseHeader := http.Header{}
	writeRouteTraceHeaders(responseHeader, routeTarget)
	downstream, err := directProxyWebSocketUpgrader.Upgrade(w, req, responseHeader)
	if err != nil {
		return err
	}
	defer downstream.Close()

	ctx := req.Context()
	clientMessages := make(chan directProxyWSMessage, 128)
	clientErr := make(chan error, 1)
	go func() {
		for {
			messageType, payload, err := downstream.ReadMessage()
			if err != nil {
				clientErr <- err
				return
			}
			msg := directProxyWSMessage{messageType: messageType, payload: append([]byte(nil), payload...)}
			select {
			case clientMessages <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	state := &directProxyTerminalState{pending: map[int]directProxyWSMessage{}}
	for {
		upstream, err := s.dialDirectTerminalWebSocket(ctx, req, atespace, actorName, routeTarget)
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(directProxyRetryDelayValue()):
			}
			_, nextTarget, resolveErr := s.extprocSrv.routeResolver.Resolve(ctx, atespace, actorName)
			if resolveErr == nil {
				routeTarget = nextTarget
			}
			continue
		}

		reconnect, err := s.proxyDirectTerminalWebSocketOnce(ctx, downstream, upstream, clientMessages, clientErr, state)
		_ = upstream.Close()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) || errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if !reconnect {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(directProxyRetryDelayValue()):
		}
		_, nextTarget, resolveErr := s.extprocSrv.routeResolver.Resolve(ctx, atespace, actorName)
		if resolveErr == nil {
			routeTarget = nextTarget
		}
	}
}

func (s *RouterServer) dialDirectTerminalWebSocket(ctx context.Context, req *http.Request, atespace, actorName string, routeTarget routeTarget) (*websocket.Conn, error) {
	targetAddr := routeTarget.HostPort()
	dialURL := "ws://" + targetAddr + req.URL.RequestURI()
	header := filteredWebSocketUpstreamHeader(req.Header)
	dialer := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	conn, _, err := dialer.DialContext(ctx, dialURL, header)
	if err != nil {
		slog.WarnContext(ctx, "direct HTTP proxy terminal websocket upstream dial failed",
			slog.String("atespace", atespace),
			slog.String("actor", actorName),
			slog.String("target", targetAddr),
			slog.Any("err", err))
		return nil, err
	}
	return conn, nil
}

func (s *RouterServer) proxyDirectTerminalWebSocketOnce(ctx context.Context, downstream, upstream *websocket.Conn, clientMessages <-chan directProxyWSMessage, clientErr <-chan error, state *directProxyTerminalState) (bool, error) {
	if state == nil {
		state = &directProxyTerminalState{pending: map[int]directProxyWSMessage{}}
	}
	if state.pending == nil {
		state.pending = map[int]directProxyWSMessage{}
	}
	if err := replayPendingTerminalMessages(upstream, state.pending); err != nil {
		return true, nil
	}
	upstreamMessages := make(chan directProxyWSMessage, 128)
	upstreamErr := make(chan error, 1)
	go func() {
		for {
			_ = upstream.SetReadDeadline(time.Now().Add(directProxyWebSocketIdleTimeoutValue()))
			messageType, payload, err := upstream.ReadMessage()
			if err != nil {
				upstreamErr <- err
				return
			}
			msg := directProxyWSMessage{messageType: messageType, payload: append([]byte(nil), payload...)}
			select {
			case upstreamMessages <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return false, nil
		case err := <-clientErr:
			if err == nil || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return false, nil
			}
			return false, err
		case err := <-upstreamErr:
			if isDirectProxyWebSocketReconnectErr(err) {
				drainQueuedTerminalClientMessages(clientMessages, state)
				return true, nil
			}
			return false, err
		case msg := <-clientMessages:
			if seq, ok := terminalClientSequence(msg.payload); ok {
				state.pending[seq] = msg
			}
			_ = upstream.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := upstream.WriteMessage(msg.messageType, msg.payload); err != nil {
				drainQueuedTerminalClientMessages(clientMessages, state)
				return true, nil
			}
		case msg := <-upstreamMessages:
			if ack, ok := terminalEchoSequence(msg.payload); ok {
				delete(state.pending, ack)
			}
			if tick, ok := terminalTickSequence(msg.payload); ok {
				if tick <= state.lastTick {
					continue
				}
				state.lastTick = tick
			}
			_ = downstream.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := downstream.WriteMessage(msg.messageType, msg.payload); err != nil {
				return false, err
			}
		}
	}
}

func drainQueuedTerminalClientMessages(clientMessages <-chan directProxyWSMessage, state *directProxyTerminalState) {
	if state == nil {
		return
	}
	if state.pending == nil {
		state.pending = map[int]directProxyWSMessage{}
	}
	for {
		select {
		case msg := <-clientMessages:
			if seq, ok := terminalClientSequence(msg.payload); ok {
				state.pending[seq] = msg
			}
		default:
			return
		}
	}
}

func replayPendingTerminalMessages(upstream *websocket.Conn, pending map[int]directProxyWSMessage) error {
	if len(pending) == 0 {
		return nil
	}
	seqs := make([]int, 0, len(pending))
	for seq := range pending {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	for _, seq := range seqs {
		msg := pending[seq]
		_ = upstream.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err := upstream.WriteMessage(msg.messageType, msg.payload); err != nil {
			return err
		}
	}
	return nil
}

func filteredWebSocketUpstreamHeader(in http.Header) http.Header {
	out := http.Header{}
	for k, values := range in {
		lower := strings.ToLower(k)
		if lower == "connection" || lower == "upgrade" || lower == "host" || strings.HasPrefix(lower, "sec-websocket-") {
			continue
		}
		for _, value := range values {
			out.Add(k, value)
		}
	}
	return out
}

func isDirectProxyTerminalWebSocket(req *http.Request) bool {
	return req.URL.Path == "/terminal/ws"
}

func isDirectProxyWebSocketReconnectErr(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || websocket.IsCloseError(err, websocket.CloseAbnormalClosure, websocket.CloseGoingAway)
}

func terminalClientSequence(payload []byte) (int, bool) {
	const prefix = "client-seq="
	text := strings.TrimSpace(string(payload))
	if !strings.HasPrefix(text, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(text, prefix)
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	seq, err := strconv.Atoi(rest[:end])
	if err != nil || seq <= 0 {
		return 0, false
	}
	return seq, true
}

func terminalEchoSequence(payload []byte) (int, bool) {
	idx := bytes.Index(payload, []byte("client-seq="))
	if idx < 0 {
		return 0, false
	}
	start := idx + len("client-seq=")
	end := start
	for end < len(payload) && payload[end] >= '0' && payload[end] <= '9' {
		end++
	}
	if end == start {
		return 0, false
	}
	seq, err := strconv.Atoi(string(payload[start:end]))
	if err != nil || seq <= 0 {
		return 0, false
	}
	return seq, true
}

func terminalTickSequence(payload []byte) (int64, bool) {
	var frame struct {
		Type             string `json:"type"`
		TerminalSequence int64  `json:"terminal_sequence"`
	}
	if err := json.Unmarshal(payload, &frame); err != nil {
		return 0, false
	}
	if frame.Type != "tick" || frame.TerminalSequence <= 0 {
		return 0, false
	}
	return frame.TerminalSequence, true
}

func (s *RouterServer) openDirectProxyUpstream(req *http.Request, actorKey string, routeTarget routeTarget, bounded bool) (*http.Response, func(), error) {
	targetHost := routeTarget.HostPort()
	out := req.Clone(req.Context())
	out.URL.Scheme = "http"
	out.URL.Host = targetHost
	out.Host = targetHost
	out.RequestURI = ""
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, nil, err
		}
		out.Body = body
	}

	release, err := s.acquireDirectProxyUpstreamSlot(req.Context(), actorKey, isDirectProxyEventStreamRequest(req))
	if err != nil {
		return nil, nil, err
	}
	cancel := func() {}
	if bounded {
		attemptCtx, cancelFn := context.WithTimeout(req.Context(), directProxyMigrationAttemptTimeoutValue())
		out = out.WithContext(attemptCtx)
		cancel = cancelFn
	}
	cancelAndRelease := func() {
		cancel()
		release()
	}
	transport := s.directProxyTransport
	if transport == nil {
		transport = directProxyDefaultTransport
	}
	resp, err := transport.RoundTrip(out)
	if err != nil {
		cancelAndRelease()
		return nil, nil, err
	}
	return resp, cancelAndRelease, nil
}

func prepareDirectProxyReplayBody(req *http.Request) error {
	if req.Method != http.MethodPost || req.Header.Get("Idempotency-Key") == "" {
		return nil
	}
	if req.Body == nil || req.Body == http.NoBody || req.GetBody != nil {
		return nil
	}

	var buf bytes.Buffer
	n, err := io.CopyN(&buf, req.Body, directProxyMaxReplayBodyBytes+1)
	_ = req.Body.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n > directProxyMaxReplayBodyBytes {
		return errDirectProxyReplayBodyTooLarge
	}
	body := append([]byte(nil), buf.Bytes()...)
	req.ContentLength = int64(len(body))
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	return nil
}

func shouldStreamDirectProxyResponse(resp *http.Response) bool {
	return strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

func (s *RouterServer) streamDirectProxyResponse(w http.ResponseWriter, req *http.Request, atespace, actorName string, resp *http.Response, cancelUpstream func(), routeTarget routeTarget) error {
	copyHeader(w.Header(), resp.Header)
	writeRouteTraceHeaders(w.Header(), routeTarget)
	w.WriteHeader(resp.StatusCode)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	var lastSSESequence int64
	deadline := time.Now().Add(directProxyRetryBudgetValue())
	for {
		seq, err := copySSEStream(w, req, resp.Body, lastSSESequence, directProxySSEIdleTimeoutValue())
		_ = resp.Body.Close()
		cancelUpstream()
		if seq > lastSSESequence {
			lastSSESequence = seq
		}
		if req.Context().Err() != nil {
			return nil
		}
		if err != nil && !errors.Is(err, io.EOF) {
			slog.WarnContext(req.Context(), "direct HTTP proxy SSE upstream stream ended with error",
				slog.String("atespace", atespace),
				slog.String("actor", actorName),
				slog.String("target", routeTarget.HostPort()),
				slog.Any("err", err))
		}
		if time.Now().After(deadline) {
			return err
		}
		select {
		case <-req.Context().Done():
			return nil
		case <-time.After(directProxyRetryDelayValue()):
		}
		_, nextTarget, resolveErr := s.extprocSrv.routeResolver.Resolve(req.Context(), atespace, actorName)
		if resolveErr != nil {
			continue
		}
		actorKey := atespace + "/" + actorName
		nextReq := req
		if lastSSESequence > 0 {
			nextReq = req.Clone(req.Context())
			nextReq.Header = req.Header.Clone()
			nextReq.Header.Set("Last-Event-ID", strconv.FormatInt(lastSSESequence, 10))
		}
		nextResp, nextCancel, openErr := s.openDirectProxyUpstream(nextReq, actorKey, nextTarget, shouldBoundDirectProxyAttempt(nextReq, nextTarget.Phase, false))
		if openErr != nil {
			continue
		}
		if !shouldStreamDirectProxyResponse(nextResp) {
			_ = nextResp.Body.Close()
			nextCancel()
			return fmt.Errorf("reconnected SSE upstream returned content-type %q", nextResp.Header.Get("Content-Type"))
		}
		cancelUpstream = nextCancel
		resp = nextResp
		routeTarget = nextTarget
	}
}

func copySSEStream(w http.ResponseWriter, req *http.Request, r io.Reader, lastSequence int64, idleTimeout time.Duration) (int64, error) {
	lines := scanSSELines(r)
	var event []string
	for {
		line, err := readSSELine(req.Context(), lines, idleTimeout)
		if err != nil {
			if len(event) > 0 {
				seq, hasSeq := sseEventSequence(event)
				if !hasSeq || seq > lastSequence {
					if _, writeErr := writeSSEEvent(w, event); writeErr != nil {
						if req.Context().Err() != nil {
							return lastSequence, nil
						}
						return lastSequence, writeErr
					}
					if hasSeq {
						lastSequence = seq
					}
				}
			}
			return lastSequence, err
		}
		if line != "" {
			event = append(event, line)
			continue
		}
		seq, hasSeq := sseEventSequence(event)
		if !hasSeq || seq > lastSequence {
			if _, err := writeSSEEvent(w, event); err != nil {
				if req.Context().Err() != nil {
					return lastSequence, nil
				}
				return lastSequence, err
			}
			if hasSeq {
				lastSequence = seq
			}
		}
		event = event[:0]
	}
}

type sseLineResult struct {
	line string
	err  error
}

func scanSSELines(r io.Reader) <-chan sseLineResult {
	out := make(chan sseLineResult, 1)
	go func() {
		defer close(out)
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			out <- sseLineResult{line: scanner.Text()}
		}
		if err := scanner.Err(); err != nil {
			out <- sseLineResult{err: err}
			return
		}
		out <- sseLineResult{err: io.EOF}
	}()
	return out
}

func readSSELine(ctx context.Context, lines <-chan sseLineResult, idleTimeout time.Duration) (string, error) {
	if idleTimeout <= 0 {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case result, ok := <-lines:
			if !ok {
				return "", io.EOF
			}
			return result.line, result.err
		}
	}
	timer := time.NewTimer(idleTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case result, ok := <-lines:
		if !ok {
			return "", io.EOF
		}
		return result.line, result.err
	case <-timer.C:
		return "", fmt.Errorf("SSE upstream idle timeout after %s", idleTimeout)
	}
}

func writeSSEEvent(w http.ResponseWriter, event []string) (int, error) {
	n := 0
	for _, line := range event {
		written, err := fmt.Fprintf(w, "%s\n", line)
		n += written
		if err != nil {
			return n, err
		}
	}
	written, err := io.WriteString(w, "\n")
	n += written
	if err != nil {
		return n, err
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return n, nil
}

func sseEventSequence(event []string) (int64, bool) {
	for _, line := range event {
		if strings.HasPrefix(line, "id:") {
			seq, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "id:")), 10, 64)
			if err == nil {
				return seq, true
			}
		}
	}
	for _, line := range event {
		if strings.HasPrefix(line, "data:") {
			var frame struct {
				Sequence int64 `json:"sequence"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &frame); err == nil && frame.Sequence > 0 {
				return frame.Sequence, true
			}
		}
	}
	return 0, false
}

func directProxyStaleCacheEnabled() bool {
	return os.Getenv("ATENET_DIRECT_PROXY_STALE_CACHE") == "1"
}

func directProxyMigrationAttemptTimeoutValue() time.Duration {
	return positiveMillisecondsEnv("ATENET_DIRECT_PROXY_MIGRATION_ATTEMPT_TIMEOUT_MS", directProxyMigrationAttemptTimeout)
}

func directProxyRetryBudgetValue() time.Duration {
	return positiveMillisecondsEnv("ATENET_DIRECT_PROXY_RETRY_BUDGET_MS", directProxyRetryBudget)
}

func directProxyRetryDelayValue() time.Duration {
	return positiveMillisecondsEnv("ATENET_DIRECT_PROXY_RETRY_DELAY_MS", directProxyRetryDelay)
}

func directProxySSEIdleTimeoutValue() time.Duration {
	return nonNegativeMillisecondsEnv("ATENET_DIRECT_PROXY_SSE_IDLE_TIMEOUT_MS", directProxySSEIdleTimeout)
}

func directProxyWebSocketIdleTimeoutValue() time.Duration {
	return nonNegativeMillisecondsEnv("ATENET_DIRECT_PROXY_WEBSOCKET_IDLE_TIMEOUT_MS", directProxyWebSocketIdleTimeout)
}

func directProxyBoundActiveRetryableAttemptEnabled() bool {
	return os.Getenv("ATENET_DIRECT_PROXY_BOUND_ACTIVE_RETRYABLE_ATTEMPT") == "1"
}

func directProxyMaxUpstreamPerActorValue() int {
	return positiveIntEnv("ATENET_DIRECT_PROXY_MAX_UPSTREAM_PER_ACTOR", directProxyMaxUpstreamPerActor)
}

func directProxyMaxStreamPerActorValue() int {
	return positiveIntEnv("ATENET_DIRECT_PROXY_MAX_STREAMS_PER_ACTOR", directProxyMaxStreamPerActor)
}

func directProxyMaxConnsPerHostValue() int {
	return positiveIntEnv("ATENET_DIRECT_PROXY_MAX_CONNS_PER_HOST", directProxyMaxConnsPerHost)
}

func positiveMillisecondsEnv(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}

func positiveIntEnv(name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return def
	}
	return value
}

func nonNegativeMillisecondsEnv(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}

func newDirectProxyDefaultTransport() http.RoundTripper {
	maxConnsPerHost := directProxyMaxConnsPerHostValue()
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   maxConnsPerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
}

type directProxyUpstreamLimiter struct {
	mu    sync.Mutex
	gates map[string]chan struct{}
}

func newDirectProxyUpstreamLimiter() *directProxyUpstreamLimiter {
	return &directProxyUpstreamLimiter{gates: map[string]chan struct{}{}}
}

func (s *RouterServer) acquireDirectProxyUpstreamSlot(ctx context.Context, actorKey string, stream bool) (func(), error) {
	limiter := s.directProxyLimiter
	if limiter == nil {
		limiter = newDirectProxyUpstreamLimiter()
		s.directProxyLimiter = limiter
	}
	if stream {
		return limiter.acquire(ctx, actorKey+"/stream", directProxyMaxStreamPerActorValue())
	}
	return limiter.acquire(ctx, actorKey, directProxyMaxUpstreamPerActorValue())
}

func (l *directProxyUpstreamLimiter) acquire(ctx context.Context, actorKey string, limit int) (func(), error) {
	if limit <= 0 {
		return func() {}, nil
	}
	l.mu.Lock()
	gate := l.gates[actorKey]
	if gate == nil || cap(gate) != limit {
		gate = make(chan struct{}, limit)
		l.gates[actorKey] = gate
	}
	l.mu.Unlock()

	select {
	case gate <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-gate
			})
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func writeRouteTraceHeaders(dst http.Header, target routeTarget) {
	if target.Generation > 0 {
		dst.Set(directProxyRouteGenerationHeader, strconv.FormatInt(target.Generation, 10))
	}
	if target.Phase != ateapipb.ActorRoute_PHASE_UNSPECIFIED {
		dst.Set(directProxyRoutePhaseHeader, target.Phase.String())
	}
	if target.Namespace != "" || target.Pod != "" {
		dst.Set(directProxyRouteTargetWorkerHeader, target.Namespace+"/"+target.Pod)
	}
	if target.Node != "" {
		dst.Set(directProxyRouteTargetNodeHeader, target.Node)
	}
	if target.IP != "" {
		dst.Set(directProxyRouteTargetIPHeader, target.IP)
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

func shouldBoundDirectProxyAttempt(req *http.Request, phase ateapipb.ActorRoute_Phase, cachedGET bool) bool {
	switch phase {
	case ateapipb.ActorRoute_PHASE_DRAINING, ateapipb.ActorRoute_PHASE_SWITCHED:
		return true
	default:
		return cachedGET || shouldBoundActiveDirectProxyAttempt(req, phase)
	}
}

func shouldBoundActiveDirectProxyAttempt(req *http.Request, phase ateapipb.ActorRoute_Phase) bool {
	if phase != ateapipb.ActorRoute_PHASE_ACTIVE || !isDirectProxyRetryable(req) {
		return false
	}
	if isDirectProxyEventStreamRequest(req) {
		return false
	}
	return directProxyBoundActiveRetryableAttemptEnabled() || req.Header.Get(directProxyBoundAttemptHeader) == "true"
}

func isDirectProxyEventStreamRequest(req *http.Request) bool {
	return strings.Contains(strings.ToLower(req.Header.Get("Accept")), "text/event-stream")
}

func isDirectProxyUpgradeRequest(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Upgrade"), "websocket") && strings.Contains(strings.ToLower(req.Header.Get("Connection")), "upgrade")
}

func isDirectProxyRetryable(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	case http.MethodPost:
		return req.Header.Get("Idempotency-Key") != ""
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
