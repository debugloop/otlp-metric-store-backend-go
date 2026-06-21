package main

import (
	"context"
	"net"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// blockingStore holds each InsertMetadata call until release is closed, signalling
// entered once it is in-flight — used to pin the concurrency limiter's slot.
type blockingStore struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingStore) CreateTables(context.Context) error { return nil }
func (b *blockingStore) InsertMetadata(context.Context, []MetricMetadataRow) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return nil
}
func (b *blockingStore) InsertGauge(context.Context, []GaugeDataRow) error { return nil }
func (b *blockingStore) InsertSum(context.Context, []SumDataRow) error     { return nil }
func (b *blockingStore) Close() error                                      { return nil }

func testServerConfig(maxInFlight int) grpcServerConfig {
	return grpcServerConfig{
		maxRecvMsgSize:        16 << 20,
		maxConcurrentStreams:  100,
		maxConnectionAge:      time.Hour,
		maxConnectionAgeGrace: time.Minute,
		maxInFlightExports:    maxInFlight,
		overloadRetryDelay:    100 * time.Millisecond,
	}
}

// startTestServer runs the real newGRPCServer over an in-memory bufconn and
// returns a connected client. Server and connection are torn down via t.Cleanup.
func startTestServer(t testing.TB, store MetricsStore, cfg grpcServerConfig) colmetricspb.MetricsServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := newGRPCServer(store, cfg)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialing test server: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})
	return colmetricspb.NewMetricsServiceClient(conn)
}

// TestServerShedsLoadOverGRPC drives the real wired server: with one in-flight
// slot, a second concurrent Export is shed with RESOURCE_EXHAUSTED + RetryInfo
// (the limiter interceptor, status mapping, and details all exercised over the wire).
func TestServerShedsLoadOverGRPC(t *testing.T) {
	store := &blockingStore{entered: make(chan struct{}, 1), release: make(chan struct{})}
	client := startTestServer(t, store, testServerConfig(1))

	now := uint64(time.Now().UnixNano())
	req := gaugeRequest("cpu", gaugeDP(now, 1))

	// First call occupies the only slot (blocks inside InsertMetadata).
	errc := make(chan error, 1)
	go func() {
		_, err := client.Export(context.Background(), req)
		errc <- err
	}()
	<-store.entered

	// Second concurrent call must be shed.
	_, err := client.Export(context.Background(), req)
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

	// Release the held call; it should complete cleanly.
	close(store.release)
	if err := <-errc; err != nil {
		t.Fatalf("first call should have succeeded: %v", err)
	}
}

// TestServerPartialSuccessOverGRPC confirms validation -> partial_success survives
// the round trip through the real server and client.
func TestServerPartialSuccessOverGRPC(t *testing.T) {
	client := startTestServer(t, &fakeStore{}, testServerConfig(10))
	now := uint64(time.Now().UnixNano())

	resp, err := client.Export(context.Background(), gaugeRequest("cpu", gaugeDP(now, 1), gaugeDP(0, 2)))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := resp.GetPartialSuccess().GetRejectedDataPoints(); got != 1 {
		t.Fatalf("expected 1 rejected datapoint over the wire, got %d", got)
	}
}
