package spilo_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HydrasDB/hydra/acceptance/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joeshaw/envdecode"
	"github.com/rs/xid"
)

type Config struct {
	Image                string        `env:"SPILO_IMAGE,required"`
	UpgradeFromImage     string        `env:"SPILO_UPGRADE_FROM_IMAGE,required"`
	ContainerLogDir      string        `env:"CONTAINER_LOG_DIR,default="`
	WaitForStartTimeout  time.Duration `env:"WAIT_FOR_START_TIMEOUT,default=60s"`
	WaitForStartInterval time.Duration `env:"WAIT_FOR_START_INTERVAL,default=5s"`
	PostgresVersion      string        `env:"SPILO_POSTGRES_VERSION,default=13"`
	PostgresPort         int           `env:"POSTGRES_PORT,default=5432"`
	ReadinessPort        int           `env:"READINESS_PORT,default=8008"`
}

var config Config

const (
	pgusername = "postgres"
	pgpassword = "hydra"
	pgrootdir  = "/home/postgres/pgroot"
	pgdatadir  = "/home/postgres/pgroot/pgdata"
)

func TestMain(m *testing.M) {
	if err := envdecode.StrictDecode(&config); err != nil {
		log.Fatal(err)
	}

	shared.MustHaveValidContainerLogDir(config.ContainerLogDir)

	os.Exit(m.Run())
}

type spiloAcceptanceContainer struct {
	config    Config
	pgdataDir string

	containerName string
	pool          *pgxpool.Pool
}

func (c *spiloAcceptanceContainer) StartContainer(t *testing.T, ctx context.Context, img string) {
	c.containerName = fmt.Sprintf("spilo-%s", xid.New())

	cmd := []string{
		"run", "--rm", "--detach", "--name", c.containerName,
		"--publish", fmt.Sprintf("%d:5432", c.config.PostgresPort),
		"--publish", fmt.Sprintf("%d:8008", c.config.ReadinessPort),
		"--env", fmt.Sprintf("PGUSER_SUPERUSER=%s", pgusername),
		"--env", fmt.Sprintf("PGPASSWORD_SUPERUSER=%s", pgpassword),
		"--env", fmt.Sprintf("PGVERSION=%s", c.config.PostgresVersion),
	}

	if c.pgdataDir != "" {
		cmd = append(cmd,
			"--env", fmt.Sprintf("PGROOT=%s", pgrootdir),
			"--env", fmt.Sprintf("PGDATA=%s", pgdatadir),
			"--volume", fmt.Sprintf("%s:%s", c.pgdataDir, pgdatadir),
		)
	}

	cmd = append(cmd, img)

	runCmd := exec.CommandContext(ctx, "docker", cmd...)

	t.Logf("Starting container %s", c.containerName)
	if o, err := runCmd.CombinedOutput(); err != nil {
		t.Fatalf("unable to start container %s: %s", err, o)
		return
	}

	c.WaitForContainerReady(t, ctx)
}

func (c *spiloAcceptanceContainer) WaitForContainerReady(t *testing.T, ctx context.Context) {
	done := make(chan bool, 1)
	timeout := time.After(c.config.WaitForStartTimeout)
	ticker := time.NewTicker(c.config.WaitForStartInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://127.0.0.1:%d", c.config.ReadinessPort), nil)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Logf("waiting for spilo to be ready: %s", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Logf("unable to read readiness body: %s", err)
					continue
				}

				t.Logf("waiting for spilo to be ready %d: %s", resp.StatusCode, body)
				continue
			}

			pool, err := shared.CreatePGPool(t, ctx, pgusername, pgpassword, c.config.PostgresPort)
			if err != nil {
				c.TerminateContainer(t, ctx, true)
				t.Fatalf("unable to create PG Pool: %s", err)
			}

			c.pool = pool
			done <- true
		case <-timeout:
			c.TerminateContainer(t, ctx, true)
			t.Fatalf("timed out waiting for container to start after %s", c.config.WaitForStartTimeout)
		}
	}
}

func (c spiloAcceptanceContainer) TerminateContainer(t *testing.T, ctx context.Context, kill bool) {
	shared.TerminateContainer(t, ctx, c.containerName, c.config.ContainerLogDir, kill)
}

func (c spiloAcceptanceContainer) Image() string {
	return c.config.Image
}

func (c spiloAcceptanceContainer) UpgradeFromImage() string {
	return c.config.UpgradeFromImage
}

func (c spiloAcceptanceContainer) PGPool() *pgxpool.Pool {
	return c.pool
}

func Test_SpiloAcceptance(t *testing.T) {
	shared.RunAcceptanceTests(
		t, context.Background(), &spiloAcceptanceContainer{config: config}, shared.Case{
			Name: "no timescaledb ext",
			SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'timescaledb';
			`,
			Validate: func(t *testing.T, row pgx.Row) {
				var count int
				if err := row.Scan(&count); err != nil {
					t.Fatal(err)
				}

				if want, got := 0, count; want != got {
					t.Errorf("timescaledb ext should not exist")
				}
			},
		},
		shared.Case{
			Name: "ensure 20 worker processes",
			SQL:  `SHOW max_worker_processes;`,
			Validate: func(t *testing.T, row pgx.Row) {
				var workerProcesses string
				if err := row.Scan(&workerProcesses); err != nil {
					t.Fatal(err)
				}

				if want, got := "20", workerProcesses; want != got {
					t.Errorf("max_worker_processes not set to 20, set to %s", got)
				}
			},
		},
		shared.Case{
			Name: "cron should use worker processes",
			SQL:  `SHOW cron.use_background_workers;`,
			Validate: func(t *testing.T, row pgx.Row) {
				var settingValue string
				if err := row.Scan(&settingValue); err != nil {
					t.Fatal(err)
				}

				if want, got := "on", settingValue; want != got {
					t.Errorf("cron.use_background_workers not set to 'on'")
				}
			},
		},
		shared.Case{
			Name: "spilo started the expected postgres version",
			SQL:  `SHOW server_version;`,
			Validate: func(t *testing.T, row pgx.Row) {
				var version string
				if err := row.Scan(&version); err != nil {
					t.Fatal(err)
				}

				if !strings.HasPrefix(version, config.PostgresVersion) {
					t.Errorf("incorrect postgres version, got %s, expected major version %s", version, config.PostgresVersion)
				}
			},
		},
	)
}

func Test_SpiloUpgrade(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "spilo_upgrade")
	if err != nil {
		t.Fatal(err)
	}

	c := spiloAcceptanceContainer{
		config:    config,
		pgdataDir: filepath.Join(tmpdir, "pgdata"),
	}

	shared.RunUpgradeTests(t, context.Background(), &c)

	t.Cleanup(func() {
		if err := os.Remove(tmpdir); err != nil {
			t.Logf("unable to cleanup tmpdir, %s", err)
		}
	})
}
