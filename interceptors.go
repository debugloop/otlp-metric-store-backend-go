package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// recoveryInterceptor turns a panic in a unary handler into an INTERNAL error so
// a single malformed request cannot crash the server.
func recoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "recovered from panic in handler",
					slog.String("method", info.FullMethod), slog.Any("panic", r))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// semaphore is a counting semaphore for admission control. tryAcquire is
// non-blocking — it returns false when full so a saturated limiter sheds load
// rather than queueing goroutines. A plain mutex+counter is used deliberately
// over a buffered channel: the intent (a bounded in-flight counter) is explicit.
type semaphore struct {
	mu       sync.Mutex
	inFlight int
	limit    int
}

func (s *semaphore) tryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight >= s.limit {
		return false
	}
	s.inFlight++
	return true
}

func (s *semaphore) release() {
	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()
}

// concurrencyLimiter caps in-flight unary calls. Each Export blocks on
// wait_for_async_insert, so blocking here too would queue goroutines unboundedly
// under load; instead a saturated limiter sheds load with RESOURCE_EXHAUSTED +
// RetryInfo, which OTLP exporters honor as throttling rather than dropping data.
// max <= 0 disables limiting.
func concurrencyLimiter(max int, retryDelay time.Duration) grpc.UnaryServerInterceptor {
	if max <= 0 {
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	sem := &semaphore{limit: max}
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !sem.tryAcquire() {
			overloadRejectedCounter.Add(ctx, 1)
			return nil, overloadedError(retryDelay)
		}
		inFlightExportsGauge.Add(ctx, 1)
		defer func() {
			sem.release()
			inFlightExportsGauge.Add(ctx, -1)
		}()
		return handler(ctx, req)
	}
}

// overloadedError returns RESOURCE_EXHAUSTED carrying a RetryInfo detail so a
// spec-compliant OTLP exporter backs off for retryDelay before retrying.
func overloadedError(retryDelay time.Duration) error {
	st := status.New(codes.ResourceExhausted, "ingest overloaded: too many in-flight Export calls")
	withInfo, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(retryDelay)})
	if err != nil {
		return st.Err()
	}
	return withInfo.Err()
}
