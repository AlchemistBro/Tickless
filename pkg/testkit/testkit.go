// Package testkit starts throwaway infrastructure (testcontainers) for
// integration tests.
package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Postgres starts PostgreSQL 16 and returns a DSN. The container is removed
// when the test ends.
func Postgres(t testing.TB) string {
	t.Helper()
	ctx := context.Background()
	c, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("test"), postgres.WithUsername("test"), postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	return dsn
}

// Kafka starts a single-node KRaft broker, creates the topics (3 partitions
// each) and returns the broker list.
func Kafka(t testing.TB, topics ...string) []string {
	t.Helper()
	ctx := context.Background()
	c, err := kafka.Run(ctx, "confluentinc/confluent-local:7.6.1")
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if len(topics) > 0 {
		if _, err := adm.CreateTopics(ctx, 3, 1, nil, topics...); err != nil {
			t.Fatalf("create topics: %v", err)
		}
	}
	return brokers
}

// ClickHouse starts a throwaway ClickHouse server and returns the native
// protocol address (host:port), user, password and database name.
func ClickHouse(t testing.TB) (addr, user, password, database string) {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "clickhouse/clickhouse-server:24.8-alpine",
			ExposedPorts: []string{"9000/tcp", "8123/tcp"},
			Env:          map[string]string{"CLICKHOUSE_USER": "test", "CLICKHOUSE_PASSWORD": "test", "CLICKHOUSE_DB": "test", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"},
			WaitingFor:   wait.ForHTTP("/ping").WithPort("8123/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return host + ":" + port.Port(), "test", "test", "test"
}
