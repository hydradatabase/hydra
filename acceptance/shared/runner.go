package shared

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A DockerComposeManager provides a shared interface for managing the lifecycle of
// Hydra-based containers during testing.
type DockerComposeManager interface {
	// StartCompose is responsible for starting the Docker Compose that includes a Hydra-based container and its test dependencies
	// then blocking until the Hydra container is able to accept Postgres connections.
	// startEverything indicates whether to start the test dependencies besides the Hydra container.
	StartCompose(t *testing.T, ctx context.Context, img string, startEverything bool)
	// TerminateCompose handles terminating the Docker compose, typically by using
	// [TerminateCompose].
	TerminateCompose(t *testing.T, ctx context.Context, kill bool)
	// Returns the image for the [ContainerManager]s.
	Image() string
	// Returns the UpgradeFromImage when the [ContainerManager] is used for
	// upgrade tests.
	UpgradeFromImage() string
	// Returns the already established pool for the container manager, typically
	// by calling [CreatePGPool]
	PGPool() *pgxpool.Pool
}

// RunAcceptanceTests runs the shared acceptance tests for a given
// [ContainerManager] as well as any additional cases provided.
func RunAcceptanceTests(t *testing.T, ctx context.Context, cm DockerComposeManager, additionalCases ...Case) {
	cm.StartCompose(t, ctx, cm.Image(), true)
	t.Cleanup(func() {
		cm.TerminateCompose(t, ctx, true)
	})

	pool := cm.PGPool()
	ver := QueryPGVersion(t, ctx, pool)

	cases := append(AcceptanceCases(), additionalCases...)
	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			checkSkipTest(t, c, ver)

			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if val := c.Validate; val == nil {
				if _, err := pool.Exec(ctx, c.SQL); err != nil {
					t.Errorf("unable to execute %s: %s", c.SQL, err)
				}
			} else {
				val(t, pool.QueryRow(ctx, c.SQL))
			}
		})
	}
}

// RunUpgradeTests runs the shared upgrade tests for a given [ContainerManager].
func RunUpgradeTests(t *testing.T, ctx context.Context, cm DockerComposeManager) {
	t.Cleanup(func() {
		cm.TerminateCompose(t, ctx, true)
	})

	t.Run("Before Upgrade", func(t *testing.T) {
		cm.StartCompose(t, ctx, cm.UpgradeFromImage(), false)

		for _, c := range BeforeUpgradeCases {
			c := c

			pool := cm.PGPool()
			ver := QueryPGVersion(t, ctx, pool)

			t.Run(c.Name, func(t *testing.T) {
				checkSkipTest(t, c, ver)

				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				if val := c.Validate; val == nil {
					if _, err := pool.Exec(ctx, c.SQL); err != nil {
						t.Errorf("unable to execute %s: %s", c.SQL, err)
					}
				} else {
					val(t, pool.QueryRow(ctx, c.SQL))
				}
			})
		}

		cm.TerminateCompose(t, ctx, false)
	})

	t.Run("After Upgrade", func(t *testing.T) {
		cm.StartCompose(t, ctx, cm.Image(), false)

		for _, c := range AfterUpgradeCases {
			c := c

			pool := cm.PGPool()
			ver := QueryPGVersion(t, ctx, pool)

			t.Run(c.Name, func(t *testing.T) {
				checkSkipTest(t, c, ver)

				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				if val := c.Validate; val == nil {
					if _, err := pool.Exec(ctx, c.SQL); err != nil {
						t.Errorf("unable to execute %s: %s", c.SQL, err)
					}
				} else {
					val(t, pool.QueryRow(ctx, c.SQL))
				}
			})
		}
	})
}

func checkSkipTest(t *testing.T, c Case, ver PGVersion) {
	if c.Skip {
		t.Skip("Test skipped")
	}

	if len(c.TargetPGVersions) > 0 && !slices.Contains(c.TargetPGVersions, ver) {
		t.Skip("Skipping test due to unsupported PG version")
	}
}
