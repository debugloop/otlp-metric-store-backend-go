package main

import "context"

// fakeStore is an in-memory MetricsStore for unit tests: it counts inserted rows
// per table and can inject an InsertSum failure to exercise the partial
// multi-table commit path. Shared by the service and gRPC server tests.
type fakeStore struct {
	metadataRows int
	gaugeRows    int
	sumRows      int
	// failSum, when non-nil, makes InsertSum return it without recording rows,
	// simulating a sum insert that fails after metadata + gauge have committed.
	failSum error
}

func (f *fakeStore) CreateTables(context.Context) error { return nil }
func (f *fakeStore) InsertMetadata(_ context.Context, rows []MetricMetadataRow) error {
	f.metadataRows += len(rows)
	return nil
}
func (f *fakeStore) InsertGauge(_ context.Context, rows []GaugeDataRow) error {
	f.gaugeRows += len(rows)
	return nil
}
func (f *fakeStore) InsertSum(_ context.Context, rows []SumDataRow) error {
	if f.failSum != nil {
		return f.failSum
	}
	f.sumRows += len(rows)
	return nil
}
func (f *fakeStore) Close() error { return nil }
