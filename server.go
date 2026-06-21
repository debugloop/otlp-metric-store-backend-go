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

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	tlsCertFile = flag.String("tlsCertFile", envOr("TLS_CERT_FILE", ""), "Path to the PEM server certificate; enables gRPC TLS when set together with -tlsKeyFile")
	tlsKeyFile  = flag.String("tlsKeyFile", envOr("TLS_KEY_FILE", ""), "Path to the PEM private key; enables gRPC TLS when set together with -tlsCertFile")

	clickhouseAddr     = flag.String("clickhouseAddr", envOr("CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse server address (host:port)")
	clickhouseDatabase = flag.String("clickhouseDatabase", envOr("CLICKHOUSE_DATABASE", "otel"), "ClickHouse database name")
	clickhouseUsername = flag.String("clickhouseUsername", envOr("CLICKHOUSE_USERNAME", "default"), "ClickHouse username")
	clickhousePassword = flag.String("clickhousePassword", envOr("CLICKHOUSE_PASSWORD", ""), "ClickHouse password")
)

const name = "dash0.com/otlp-log-processor-backend"

var (
	meter                  = otel.Meter(name)
	logger                 = otelslog.NewLogger(name)
	metricsReceivedCounter metric.Int64Counter
)

func init() {
	var err error
	metricsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.received",
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
	if err != nil {
		panic(err)
	}
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
	storeCtx := context.Background()
	store, err := NewClickHouseMetricsStore(storeCtx, *clickhouseAddr, *clickhouseDatabase, *clickhouseUsername, *clickhousePassword)
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

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer(*listenAddr, store))

	slog.Debug("Starting gRPC server")

	return grpcServer.Serve(listener)
}
