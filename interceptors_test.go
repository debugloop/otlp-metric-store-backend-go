package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestConcurrencyLimiterShedsLoad verifies that once the in-flight bound is
// reached, further calls are rejected with RESOURCE_EXHAUSTED carrying RetryInfo,
// rather than blocked or dropped.
func TestConcurrencyLimiterShedsLoad(t *testing.T) {
	limiter := concurrencyLimiter(1, 250*time.Millisecond)
	info := &grpc.UnaryServerInfo{FullMethod: "/dash0.MetricsService/Export"}

	entered := make(chan struct{})
	release := make(chan struct{})
	blocking := func(context.Context, any) (any, error) {
		close(entered)
		<-release
		return "ok", nil
	}

	// Occupy the single slot.
	done := make(chan struct{})
	go func() {
		_, _ = limiter(context.Background(), nil, info, blocking)
		close(done)
	}()
	<-entered

	// A second concurrent call must be shed.
	_, err := limiter(context.Background(), nil, info,
		func(context.Context, any) (any, error) { return "ok", nil })
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected RESOURCE_EXHAUSTED, got code=%s err=%v", status.Code(err), err)
	}

	hasRetryInfo := false
	for _, d := range status.Convert(err).Details() {
		if _, ok := d.(*errdetails.RetryInfo); ok {
			hasRetryInfo = true
		}
	}
	if !hasRetryInfo {
		t.Fatal("expected a RetryInfo detail on the shed response")
	}

	// Release the held slot and wait for it to drain; the next call is admitted.
	close(release)
	<-done
	if _, err := limiter(context.Background(), nil, info,
		func(context.Context, any) (any, error) { return "ok", nil }); err != nil {
		t.Fatalf("expected admission after slot freed, got %v", err)
	}
}

// TestConcurrencyLimiterDisabled verifies max<=0 passes everything through.
func TestConcurrencyLimiterDisabled(t *testing.T) {
	limiter := concurrencyLimiter(0, time.Second)
	info := &grpc.UnaryServerInfo{FullMethod: "/x/Y"}
	for range 5 {
		if _, err := limiter(context.Background(), nil, info,
			func(context.Context, any) (any, error) { return "ok", nil }); err != nil {
			t.Fatalf("disabled limiter should not reject, got %v", err)
		}
	}
}
