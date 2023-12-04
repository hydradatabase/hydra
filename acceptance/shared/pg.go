package shared

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	regexpPGVersion = regexp.MustCompile(`^PostgreSQL (\d+)`)
)

func QueryPGVersion(ctx context.Context, pool *pgxpool.Pool) (PGVersion, error) {
	row := pool.QueryRow(ctx, "SELECT VERSION();")

	var version string
	if err := row.Scan(&version); err != nil {
		return PGVersionUnknown, err
	}

	matches := regexpPGVersion.FindStringSubmatch(version)
	if len(matches) == 0 {
		return PGVersionUnknown, fmt.Errorf("failed to parse pg_config --version output: %s", version)
	}

	return PGVersion(matches[1]), nil
}
