package shared

import (
	"context"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	regexpPGVersion = regexp.MustCompile(`^PostgreSQL (\d+)`)
)

func QueryPGVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool) PGVersion {
	row := pool.QueryRow(ctx, "SELECT VERSION();")

	var version string
	if err := row.Scan(&version); err != nil {
		t.Fatal(err)
	}

	matches := regexpPGVersion.FindStringSubmatch(version)
	if len(matches) == 0 {
		t.Fatalf("failed to parse pg_config --version output: %s", version)
	}

	return PGVersion(matches[1])
}
