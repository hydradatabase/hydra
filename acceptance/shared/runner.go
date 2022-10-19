package shared

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ContainerManager interface {
	StartContainer(t *testing.T, ctx context.Context, img string)
	TerminateContainer(t *testing.T, ctx context.Context, kill bool)
	Image() string
	UpgradeFromImage() string
	PGPool() *pgxpool.Pool
}

func RunAcceptanceTests(t *testing.T, ctx context.Context, cm ContainerManager, additionalCases ...Case) {
	cm.StartContainer(t, ctx, cm.Image())
	t.Cleanup(func() {
		cm.TerminateContainer(t, ctx, true)
	})

	pool := cm.PGPool()

	cases := append(AcceptanceCases, additionalCases...)

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if val := c.Validate; val == nil {
				if _, err := pool.Exec(ctx, c.SQL); err != nil {
					t.Fatal(err)
				}
			} else {
				val(t, pool.QueryRow(ctx, c.SQL))
			}
		})
	}
}

func RunUpgradeTests(t *testing.T, ctx context.Context, cm ContainerManager) {
	t.Cleanup(func() {
		cm.TerminateContainer(t, ctx, true)
	})

	t.Run("Before Upgrade", func(t *testing.T) {
		cm.StartContainer(t, ctx, cm.UpgradeFromImage())

		for _, c := range BeforeUpgradeCases {
			pool := cm.PGPool()

			t.Run(c.Name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				if val := c.Validate; val == nil {
					if _, err := pool.Exec(ctx, c.SQL); err != nil {
						t.Fatal(err)
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
			pool := cm.PGPool()

			t.Run(c.Name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				if val := c.Validate; val == nil {
					if _, err := pool.Exec(ctx, c.SQL); err != nil {
						t.Fatal(err)
					}
				} else {
					val(t, pool.QueryRow(ctx, c.SQL))
				}
			})
		}
	})
}
