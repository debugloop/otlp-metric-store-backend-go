package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// envOr returns the value of environment variable key, or fallback if unset/empty.
// Used so flag defaults can be supplied via the environment (12-factor config).
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")

	clickhouseAddr     = flag.String("clickhouseAddr", envOr("CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse server address (host:port)")
	clickhouseDatabase = flag.String("clickhouseDatabase", envOr("CLICKHOUSE_DATABASE", "otel"), "ClickHouse database name")
	clickhouseUsername = flag.String("clickhouseUsername", envOr("CLICKHOUSE_USERNAME", "default"), "ClickHouse username")
	clickhousePassword = flag.String("clickhousePassword", envOr("CLICKHOUSE_PASSWORD", ""), "ClickHouse password")

	clickhouseMaxOpenConns = flag.Int("clickhouseMaxOpenConns", 32, "Max ClickHouse connections; keep >= maxInFlightExports so admitted requests don't queue on the pool")
	insertTimeout          = flag.Duration("insertTimeout", 30*time.Second, "Per-insert deadline; bounds how long a slow backend can hold an admission slot")

	maxInFlightExports    = flag.Int("maxInFlightExports", 32, "Max concurrent in-flight Export calls before shedding load (keep <= clickhouseMaxOpenConns; bounds memory at ~maxInFlightExports * maxReceiveMessageSize)")
	overloadRetryDelay    = flag.Duration("overloadRetryDelay", time.Second, "RetryInfo delay hint returned to clients when the ingest is overloaded")
	maxConcurrentStreams  = flag.Uint("maxConcurrentStreams", 1000, "Max concurrent HTTP/2 streams per connection")
	maxConnectionAge      = flag.Duration("maxConnectionAge", 30*time.Minute, "Max age of a gRPC connection before graceful close (lets L4 LBs rebalance exporters)")
	maxConnectionAgeGrace = flag.Duration("maxConnectionAgeGrace", time.Minute, "Grace period for in-flight RPCs after maxConnectionAge")
	shutdownTimeout       = flag.Duration("shutdownTimeout", 25*time.Second, "Max time to drain in-flight RPCs on SIGTERM/SIGINT before forcing stop")
)

const name = "dash0.com/otlp-log-processor-backend"

var (
	meter  = otel.Meter(name)
	tracer = otel.Tracer(name)
	logger = otelslog.NewLogger(name)

	// Ingest-path self-telemetry. Attributes are kept low-cardinality
	// (metric.type ∈ {gauge,sum}, table ∈ {metadata,gauge,sum}) — never the
	// metric name — so the app's own metrics don't explode in cardinality.
	metricsReceivedCounter      metric.Int64Counter
	metadataRowsInsertedCounter metric.Int64Counter
	dataPointsInsertedCounter   metric.Int64Counter
	rejectedDataPointsCounter   metric.Int64Counter
	inFlightExportsGauge        metric.Int64UpDownCounter
	overloadRejectedCounter     metric.Int64Counter
	insertLatencyHist           metric.Float64Histogram
)

func init() {
	var err error
	mustErr := func() {
		if err != nil {
			panic(err)
		}
	}

	metricsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.received",
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
	mustErr()
	metadataRowsInsertedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metadata_rows.inserted",
		metric.WithDescription("Metadata lookup rows inserted into otel_metrics_metadata"),
		metric.WithUnit("{row}"))
	mustErr()
	dataPointsInsertedCounter, err = meter.Int64Counter("com.dash0.homeexercise.datapoints.inserted",
		metric.WithDescription("Datapoints inserted into the slim data tables"),
		metric.WithUnit("{datapoint}"))
	mustErr()
	rejectedDataPointsCounter, err = meter.Int64Counter("com.dash0.homeexercise.datapoints.rejected",
		metric.WithDescription("Datapoints rejected by validation and reported via partial_success"),
		metric.WithUnit("{datapoint}"))
	mustErr()
	inFlightExportsGauge, err = meter.Int64UpDownCounter("com.dash0.homeexercise.exports.in_flight",
		metric.WithDescription("In-flight Export calls admitted by the concurrency limiter"),
		metric.WithUnit("{rpc}"))
	mustErr()
	overloadRejectedCounter, err = meter.Int64Counter("com.dash0.homeexercise.exports.overload_rejected",
		metric.WithDescription("Export calls shed with RESOURCE_EXHAUSTED due to the in-flight limit"),
		metric.WithUnit("{rpc}"))
	mustErr()
	insertLatencyHist, err = meter.Float64Histogram("com.dash0.homeexercise.insert.duration",
		metric.WithDescription("ClickHouse insert batch latency by table"),
		metric.WithUnit("s"))
	mustErr()
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	slog.SetDefault(logger)
	logger.Info("Starting application")

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	flag.Parse()

	// Connect to ClickHouse and bootstrap the schema. Fail fast with a clear
	// error if the store is unreachable at startup rather than accepting
	// metrics we cannot persist.
	if *maxInFlightExports > *clickhouseMaxOpenConns {
		slog.Warn("maxInFlightExports exceeds clickhouseMaxOpenConns; admitted requests may queue on the connection pool instead of being shed",
			slog.Int("maxInFlightExports", *maxInFlightExports),
			slog.Int("clickhouseMaxOpenConns", *clickhouseMaxOpenConns))
	}

	storeCtx := context.Background()
	store, err := NewClickHouseMetricsStore(storeCtx, ClickHouseConfig{
		Addr:          *clickhouseAddr,
		Database:      *clickhouseDatabase,
		Username:      *clickhouseUsername,
		Password:      *clickhousePassword,
		MaxOpenConns:  *clickhouseMaxOpenConns,
		InsertTimeout: *insertTimeout,
	})
	if err != nil {
		return fmt.Errorf("connecting to clickhouse at %s: %w", *clickhouseAddr, err)
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()
	if err := store.CreateTables(storeCtx); err != nil {
		return fmt.Errorf("creating clickhouse tables: %w", err)
	}
	slog.Info("Connected to ClickHouse",
		slog.String("addr", *clickhouseAddr),
		slog.String("database", *clickhouseDatabase))

	slog.Debug("Starting listener", slog.String("listenAddr", *listenAddr))
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return err
	}

	grpcServer := newGRPCServer(store, grpcServerConfig{
		maxRecvMsgSize:        *maxReceiveMessageSize,
		maxConcurrentStreams:  uint32(*maxConcurrentStreams),
		maxConnectionAge:      *maxConnectionAge,
		maxConnectionAgeGrace: *maxConnectionAgeGrace,
		maxInFlightExports:    *maxInFlightExports,
		overloadRetryDelay:    *overloadRetryDelay,
	})

	// Graceful shutdown: on SIGTERM/SIGINT (e.g. a pod rollout) stop accepting new
	// RPCs and drain in-flight ones — the complement to the maxConnectionAge
	// draining. A second signal, or exceeding shutdownTimeout, forces an immediate
	// stop so a stuck handler cannot block termination indefinitely.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		slog.Debug("Starting gRPC server")
		serveErr <- grpcServer.Serve(listener)
	}()

	select {
	case err = <-serveErr:
		return err
	case <-ctx.Done():
		stop() // restore default handling so a second signal force-quits
		slog.Info("shutdown signal received, draining in-flight requests",
			slog.Duration("timeout", *shutdownTimeout))
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			slog.Info("graceful shutdown complete")
		case <-time.After(*shutdownTimeout):
			slog.Warn("graceful shutdown timed out, forcing stop")
			grpcServer.Stop()
		}
		return nil
	}
}

// grpcServerConfig holds the tunables for the hardened ingest server, so the same
// construction is shared by run() and tests (no divergent test-only server).
type grpcServerConfig struct {
	maxRecvMsgSize        int
	maxConcurrentStreams  uint32
	maxConnectionAge      time.Duration
	maxConnectionAgeGrace time.Duration
	maxInFlightExports    int
	overloadRetryDelay    time.Duration
}

// newGRPCServer builds the gRPC server with connection-lifetime bounds, a stream
// cap, and the panic-recovery + concurrency-limiter interceptor chain, with the
// metrics service registered against store.
func newGRPCServer(store MetricsStore, cfg grpcServerConfig) *grpc.Server {
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(cfg.maxRecvMsgSize),
		grpc.Creds(insecure.NewCredentials()),
		grpc.MaxConcurrentStreams(cfg.maxConcurrentStreams),
		// Bound connection lifetime so L4 load balancers can rebalance the
		// long-lived connections OTLP exporters hold open.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      cfg.maxConnectionAge,
			MaxConnectionAgeGrace: cfg.maxConnectionAgeGrace,
		}),
		// Panic recovery outermost; then the in-flight limiter, which sheds load
		// with RESOURCE_EXHAUSTED + RetryInfo before the handler runs.
		grpc.ChainUnaryInterceptor(
			recoveryInterceptor(),
			concurrencyLimiter(cfg.maxInFlightExports, cfg.overloadRetryDelay),
		),
	)
	colmetricspb.RegisterMetricsServiceServer(s, newServer(*listenAddr, store))
	return s
}
