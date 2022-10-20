package shared

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A ContainerManager provides a shared interface for managing the lifecycle of
// Hydra-based containers during testing.
type ContainerManager interface {
	// StartContainer is responsible for starting a Hydra-based container and
	// then blocking until the container is able to accept Postgres connections.
	StartContainer(t *testing.T, ctx context.Context, img string)
	// TerminateContainer handles terminating the container, typically by using
	// [TerminateContainer].
	TerminateContainer(t *testing.T, ctx context.Context, kill bool)
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
func RunAcceptanceTests(t *testing.T, ctx context.Context, cm ContainerManager, additionalCases ...Case) {
	cm.StartContainer(t, ctx, cm.Image())
	t.Cleanup(func() {
		cm.TerminateContainer(t, ctx, true)
	})

	pool := cm.PGPool()

	cases := append(AcceptanceCases, additionalCases...)

	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
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
func RunUpgradeTests(t *testing.T, ctx context.Context, cm ContainerManager) {
	t.Cleanup(func() {
		cm.TerminateContainer(t, ctx, true)
	})

	t.Run("Before Upgrade", func(t *testing.T) {
		cm.StartContainer(t, ctx, cm.UpgradeFromImage())

		for _, c := range BeforeUpgradeCases {
			c := c
			pool := cm.PGPool()

			t.Run(c.Name, func(t *testing.T) {
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

		cm.TerminateContainer(t, ctx, false)
	})

	t.Run("After Upgrade", func(t *testing.T) {
		cm.StartContainer(t, ctx, cm.Image())

		for _, c := range AfterUpgradeCases {
			c := c
			pool := cm.PGPool()

			t.Run(c.Name, func(t *testing.T) {
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
