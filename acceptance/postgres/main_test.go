package postgres

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/HydrasDB/hydra/acceptance/shared"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joeshaw/envdecode"
	"github.com/rs/xid"
)

type Config struct {
	Image                string        `env:"POSTGRES_IMAGE,required"`
	UpgradeFromImage     string        `env:"POSTGRES_UPGRADE_FROM_IMAGE,required"`
	ContainerLogDir      string        `env:"CONTAINER_LOG_DIR,default="`
	WaitForStartTimeout  time.Duration `env:"WAIT_FOR_START_TIMEOUT,default=15s"`
	WaitForStartInterval time.Duration `env:"WAIT_FOR_START_INTERVAL,default=1s"`
	PostgresPort         int           `env:"POSTGRES_PORT,default=5432"`
}

var config Config

const (
	pgusername = "hydra"
	pgpassword = "hydra"
	pgdatadir  = "/var/lib/postgresql/data/pgdata"
)

func TestMain(m *testing.M) {
	if err := envdecode.StrictDecode(&config); err != nil {
		log.Fatal(err)
	}

	shared.MustHaveValidContainerLogDir(config.ContainerLogDir)

	os.Exit(m.Run())
}

type postgresAcceptanceContainer struct {
	config    Config
	pgdataDir string

	containerName string
	pool          *pgxpool.Pool
}

func (c *postgresAcceptanceContainer) StartContainer(t *testing.T, ctx context.Context, img string) {
	c.containerName = fmt.Sprintf("postgres-%s", xid.New())

	cmd := []string{
		"run", "--rm", "--detach", "--name", c.containerName,
		"--publish", fmt.Sprintf("%d:5432", c.config.PostgresPort), "--env",
		fmt.Sprintf("POSTGRES_USER=%s", pgusername), "--env",
		fmt.Sprintf("POSTGRES_PASSWORD=%s", pgpassword),
	}

	if c.pgdataDir != "" {
		cmd = append(cmd,
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

	return
}

func (c *postgresAcceptanceContainer) WaitForContainerReady(t *testing.T, ctx context.Context) {
	done := make(chan bool, 1)
	timeout := time.After(c.config.WaitForStartTimeout)
	ticker := time.NewTicker(c.config.WaitForStartInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			pool, err := shared.CreatePGPool(t, ctx, pgusername, pgpassword, c.config.PostgresPort)
			if errors.Is(err, shared.ErrPgPoolConnect) {
				continue
			} else if err != nil {
				t.Fatal(err)
			}
			c.pool = pool
			done <- true
		case <-timeout:
			c.TerminateContainer(t, ctx, true)
			t.Fatalf("timed out waiting for container to start after %s", c.config.WaitForStartTimeout)
		}
	}
}

func (c postgresAcceptanceContainer) TerminateContainer(t *testing.T, ctx context.Context, kill bool) {
	shared.TerminateContainer(t, ctx, c.containerName, c.config.ContainerLogDir, kill)
}

func (c postgresAcceptanceContainer) Image() string {
	return c.config.Image
}

func (c postgresAcceptanceContainer) UpgradeFromImage() string {
	return c.config.UpgradeFromImage
}

func (c postgresAcceptanceContainer) PGPool() *pgxpool.Pool {
	return c.pool
}

func Test_PostgresAcceptance(t *testing.T) {
	shared.RunAcceptanceTests(
		t, context.Background(), &postgresAcceptanceContainer{config: config},
	)
}

func Test_PostgresUpgrade(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "postgres_upgrade")
	if err != nil {
		t.Fatal(err)
	}

	c := postgresAcceptanceContainer{
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
